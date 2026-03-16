from rdflib import Graph, Dataset, URIRef, Literal
from rdflib.namespace import XSD, PROV, DCTERMS, RDF
from typing import List, Union, Dict, Tuple, Generator, Iterable, Optional
from meta_prov_fixer.utils import remove_seq_num, get_seq_num, normalise_datetime, get_previous_meta_dump_uri, get_described_res_omid, \
read_rdf_dump, get_rdf_prov_filepaths, batched, get_graph_uri_from_se_uri
import logging
from string import Template
import re
import json
from tqdm import tqdm
from sparqlite import SPARQLClient, QueryError, EndpointError
from typing import TextIO
from datetime import datetime, date, timezone
import os
from pathlib import Path
import logging
import traceback
import time
from zipfile import ZipFile, ZIP_DEFLATED
from enum import IntEnum


class Step(IntEnum):
    START = 0
    FILLER = 1
    DATETIME = 2
    MISSING_PS = 3
    MULTI_PA = 4
    MULTI_OBJECT = 5
    WRITE_FILE = 6


class Checkpoint:
    def __init__(self, path: str):
        self.path = path
        self.state = None
        self.dirty = False
        self.load()

    def load(self):
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as f:
                self.state = json.load(f)
        else:
            self.state = None

    def _atomic_write(self, data: dict, retries=5, delay=0.05):
        tmp_path = self.path + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        for i in range(retries):
            try:
                os.replace(tmp_path, self.path)
                return
            
            #  catch PermissionError: [WinError 5] Accesso negato: 'fix_prov.checkpoint.json.tmp' -> 'fix_prov.checkpoint.json'
            except PermissionError:
                time.sleep(delay)

        raise PermissionError(f"Failed to replace checkpoint file after {retries} retries")

    def update_state(
        self,
        file_index: int,
        file_path: str,
        step: Step,
        endpoint_done: bool,
        local_done: bool
    ):
        self.state = {
            "file_index": file_index,
            "file_path": file_path,
            "step": step.name,
            "endpoint_done": endpoint_done,
            "local_done": local_done,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        self.dirty = True

    def flush(self):
        if self.dirty and self.state:
            self._atomic_write(self.state)
            self.dirty = False

    def should_skip_file(self, idx: int) -> bool:
        return self.state and idx < self.state["file_index"]

    def step_completed(self, step: Step, file_index:int) -> bool:
        if not self.state:
            return False
        return bool((Step[self.state["step"]] >= step) and self.state["file_index"] >= file_index)


# --- Caching mechanism for filler issues detection --- #

def _atomic_json_write(path: str, data: dict):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, path)


def load_or_prepare_filler_issues(data_dir: str, cache_fp: str="filler_issues.cache.json"):

    if os.path.exists(cache_fp):
        with open(cache_fp, "r", encoding="utf-8") as f:
            cached = json.load(f)

        if cached.get("data_dir") == os.path.abspath(data_dir):
            filler_issues = [
                (URIRef(g), {
                    "to_delete": [URIRef(u) for u in v["to_delete"]],
                    "remaining_snapshots": [URIRef(u) for u in v["remaining_snapshots"]],
                })
                for g, v in cached["filler_issues"]
            ]
            return filler_issues, cached["tot_files"], cache_fp

        # data_dir mismatch -> invalidate cache
        os.remove(cache_fp)

    # Cache miss -> compute
    filler_issues, tot_files = prepare_filler_issues(data_dir)

    serializable = [
        (
            str(g),
            {
                "to_delete": [str(u) for u in d["to_delete"]],
                "remaining_snapshots": [str(u) for u in d["remaining_snapshots"]],
            }
        )
        for g, d in filler_issues
    ]

    payload = {
        "data_dir": os.path.abspath(data_dir),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "tot_files": tot_files,
        "filler_issues": serializable,
    }

    _atomic_json_write(cache_fp, payload)

    return filler_issues, tot_files, cache_fp



class FillerFixerFile:

    def __init__(self, endpoint):
        pass
        self.endpoint = endpoint

    def detect(graph:Graph) -> Optional[Tuple[URIRef, Dict[str, List[URIRef]]]]:
        """
        Detects the issues in the input graph. If no filler snapshots are found, return None. 
        Else, a 2-elements tuple is returned, where the first element is the URIRef object of the 
        graph's identifier, and the second element is a dictionary with "to_delete" and 
        "remaining_snapshots" keys, both having as their value a list of URIRef objects, respectively 
        representing the fillers snapshots that must be deleted and the snapshots that should be kept 
        (but must be renamed).

        :param graph: the named graph for the provenance of an entity
        """

        # out = (URIRef(graph.identifier), {"to_delete":[], "remaining_snapshots":[]})

        snapshots = list(graph.subjects(unique=True))
        if len(snapshots) == 1:
            return None
        
        creation_se = URIRef(str(graph.identifier) + 'se/1')
        fillers = set()
        remaining = set()

        for s in snapshots:
            if s == creation_se:
                remaining.add(s)
                continue
            if (s, URIRef('https://w3id.org/oc/ontology/hasUpdateQuery'), None) not in graph:
                if (s, DCTERMS.description, None) not in graph:
                    fillers.add(s)
                else:
                    for desc_val in graph.objects(s, DCTERMS.description, unique=True):
                        if "merged" not in str(desc_val).lower():
                            fillers.add(s)
            if s not in fillers:
                remaining.add(s)

        if not fillers:
            return None

        out = (
            URIRef(graph.identifier), 
            {
                "to_delete":list(fillers), 
                "remaining_snapshots":list(remaining)
            }
        )

        return out


    @staticmethod
    def map_se_names(to_delete:set, remaining: set) -> dict:
        """
        Associates a new URI value to each snapshot URI in the union of ``to_delete`` and ``remaining`` (containing snapshot URIs).

        Values in the mapping dictionary are not unique, i.e., multiple old URIs can be mapped to the same new URI.
        If ``to_delete`` is empty, the returned dictionary will have identical keys and values, i.e., the URIs will not change.
        Each URI in ``to_delete`` will be mapped to the new name of the URI in ``remaining`` that immediately precedes it in
        a sequence ordered by sequence number.

        **Examples:**

        .. code-block:: python

            to_delete = {'https://w3id.org/oc/meta/br/06101234191/prov/se/3'}
            remaining = {'https://w3id.org/oc/meta/br/06101234191/prov/se/1', 'https://w3id.org/oc/meta/br/06101234191/prov/se/2', 'https://w3id.org/oc/meta/br/06101234191/prov/se/4'}

            # The returned mapping will be:
            {
                'https://w3id.org/oc/meta/br/06101234191/prov/se/1': 'https://w3id.org/oc/meta/br/06101234191/prov/se/1',
                'https://w3id.org/oc/meta/br/06101234191/prov/se/2': 'https://w3id.org/oc/meta/br/06101234191/prov/se/2',
                'https://w3id.org/oc/meta/br/06101234191/prov/se/3': 'https://w3id.org/oc/meta/br/06101234191/prov/se/2',
                'https://w3id.org/oc/meta/br/06101234191/prov/se/4': 'https://w3id.org/oc/meta/br/06101234191/prov/se/3'
            }

        :param to_delete: A set of snapshot URIs that should be deleted.
        :type to_delete: set
        :param remaining: A set of URIs of snapshots that should remain in the graph (AFTER BEING RENAMED).
        :type remaining: set
        :returns: A dictionary mapping old snapshot URIs to their new URIs.
        :rtype: dict
        """
        to_delete :set = {str(el) for el in to_delete}
        remaining :set = {str(el) for el in remaining}
        all_snapshots:list = sorted(to_delete|remaining, key=lambda x: get_seq_num(x))  # sorting is required!

        mapping = {}
        sorted_remaining = []
        base_uri = remove_seq_num(all_snapshots[0])

        if not all(u.startswith(base_uri) for u in all_snapshots):
            logging.error(f"All snapshots must start with the same base URI: {base_uri}. Found: {all_snapshots}")
            raise ValueError(f"Can rename only snapshots that are included in the same named graph.")

        for old_uri in all_snapshots:
            if old_uri in remaining:
                new_uri = f"{base_uri}{len(sorted_remaining)+1}"
                mapping[old_uri] = new_uri
                sorted_remaining.append(new_uri)

            else:  # i.e., elif old_uri in to_delete
                try:
                    new_uri = f"{base_uri}{get_seq_num(sorted_remaining[-1])}"
                except IndexError:
                    # all snapshots are fillers (must be deleted), including the first one (creation)
                    logging.error(f"The first snapshot {old_uri} is a filler. Cannot rename the remaining snapshots.")

                mapping[old_uri] = new_uri

        return mapping

    @staticmethod
    def make_global_rename_map(graphs_with_fillers:Iterable):
        """
        Create a dictionary of the form <old_subject_uri>:<new_snapshot_name>.
        
        :param graphs_with_fillers: an Iterable consisting of all the graphs containing filler snapshots,
            the snapshots to delete in that graph, and the other snapshot in that same graph (part of 
            which must be renamed).
        :type graphs_with_fillers: Iterable
        """
        out = dict()

        for g, _dict in graphs_with_fillers:
            mapping = FillerFixerFile.map_se_names(_dict['to_delete'], _dict['remaining_snapshots'])
            for k, v in mapping.items():
                if k != v:
                    out[k] = v
        return out

    
    def fix_local_graph(ds: Dataset, graph:Graph, global_rename_map:dict, fillers_issues_lookup:dict) -> None:

        # delete all triples where subject is a filler (in local graph)
        for snapshot_node, _, _, _  in ds.quads((None, None, None, graph.identifier)):
            if str(snapshot_node) in global_rename_map:
                if snapshot_node in fillers_issues_lookup[graph.identifier]['to_delete']:
                    ds.remove((URIRef(snapshot_node), None, None, graph.identifier))
        
        # replace objects that used to be fillers snapshots (in this graph or in other graphs, using global mapping)
        for subj, pred, obj, _ in ds.quads((None, None, None, graph.identifier)):
            new_subj_name = subj
            new_obj_name = obj
            if str(subj) in global_rename_map:
                new_subj_name = URIRef(global_rename_map.get(str(subj), subj))
            if str(obj) in global_rename_map:
                new_obj_name = URIRef(global_rename_map.get(str(obj), obj))
            if (new_subj_name!=subj) or (new_obj_name!=obj):
                ds.remove((subj, pred, obj, graph.identifier))
                ds.add((new_subj_name, pred, new_obj_name, graph.identifier))

        # adapt invalidatedAtTime relationships (in local graph only)
        unsorted_curr_snpsts_strngs = []
        for s in ds.graph(graph.identifier).subjects(unique=True):
            unsorted_curr_snpsts_strngs.append(str(s))
        snapshots_strings:list = sorted(unsorted_curr_snpsts_strngs, key=lambda x: get_seq_num(str(x)))

        for s, following_se in zip(snapshots_strings, snapshots_strings[1:]):
            try:
                new_invaldt = min(
                    list(ds.graph(graph.identifier).objects(URIRef(following_se), PROV.generatedAtTime, unique=True)),
                    key=lambda x: normalise_datetime(str(x))
                )
            except ValueError:
                # TODO: This should be a very rare case, but consider implementing a more robust handling
                logging.error(f"Cannot find prov:generatedAtTime for snapshot {following_se} in graph {graph.identifier}. Skipping invalidatedAtTime update for snapshot {s}.")
                continue

            for old_invaldt in ds.graph(graph.identifier).objects(URIRef(s), PROV.invalidatedAtTime, unique=True):
                ds.remove((URIRef(s), PROV.invalidatedAtTime, old_invaldt, graph.identifier))
            ds.add((URIRef(s), PROV.invalidatedAtTime, new_invaldt, graph.identifier))

    def build_delete_sparql_query(local_deletions:tuple)->str:
        """
        Makes the SPARQL query text for deleting snapshots from the triplestore based on the provided deletions list.

        :param deletions: A tuple or list where the first element is a graph URI, 
            and the second is a dictionary with `'to_delete'` and `'remaining_snapshots'` sets.
        """

        deletion_template = Template("""
            $dels
        """)

        # step 1: delete filler snapshots in the role of subjects
        dels = []
        g_uri, values = local_deletions
        for se_to_delete in values['to_delete']:
            single_del = f"DELETE WHERE {{ GRAPH <{str(g_uri)}> {{ <{str(se_to_delete)}> ?p ?o . }}}};\n"
            dels.append(single_del)
        dels_str = "    ".join(dels)
        query_str = deletion_template.substitute(dels=dels_str)
        return query_str
    
    def build_rename_sparql_query(local_mapping:dict) -> str:
        """
        Makes the SPARQL query text to rename snapshots in the triplestore according to the provided mapping.

        :param local_mapping: A dictionary where keys are old snapshot URIs and values are new snapshot URIs.
        :type local_mapping: dict
        """

        mapping = dict(sorted(local_mapping.items(), key=lambda i: get_seq_num(i[0])))
        
        per_snapshot_template = Template("""
        DELETE {
          GRAPH ?g {
            <$old_uri> ?p ?o .
            ?s ?p2 <$old_uri> .
          }
        }
        INSERT {
          GRAPH ?g {
            <$new_uri> ?p ?o .
            ?s ?p2 <$new_uri> .
          }
        }
        WHERE {
          GRAPH ?g {
            {
              <$old_uri> ?p ?o .
            }
            UNION
            {
              ?s ?p2 <$old_uri> .
            }
          }
        }
        """)


        per_snapshot_portions = []
        for old_uri, new_uri in mapping.items():
            if old_uri == new_uri:
                continue
            query_portion = per_snapshot_template.substitute(old_uri=old_uri, new_uri=new_uri)
            per_snapshot_portions.append(query_portion)
        
        final_query_str = ";\n".join(per_snapshot_portions)

        return final_query_str


    def build_adapt_invaltime_sparql_query(graph_uri: str, local_snapshots: list) -> str:
        """
        Update the ``prov:invalidatedAtTime`` property of each snapshot in the provided list to match 
        the value of ``prov:generatedAtTime`` of the following snapshot.

        :param graph_uri: The URI of the named graph containing the snapshots.
        :type graph_uri: str
        :param local_snapshots: A list of snapshot URIs.
        :type local_snapshots: list
        :returns: None
        """
        
        snapshots = sorted(local_snapshots, key=lambda x: get_seq_num(x))  # sorting is required!
        per_snaphot_template = Template("""
        DELETE {
          GRAPH <$graph> { <$snapshot> prov:invalidatedAtTime ?old_time . }
        }
        INSERT {
          GRAPH <$graph> { <$snapshot> prov:invalidatedAtTime ?new_time . }
        }
        WHERE {
          GRAPH <$graph> {
            OPTIONAL {
              <$snapshot> prov:invalidatedAtTime ?old_time .
            }
            <$following_snapshot> prov:generatedAtTime ?new_time .
          }
        }
        """)

        per_snapshot_portions = []

        for s, following_se in zip(snapshots, snapshots[1:]):
            query_portion = per_snaphot_template.substitute(
                graph=graph_uri,
                snapshot=s,
                following_snapshot=following_se
            )

            per_snapshot_portions.append(query_portion)

        final_query_str = "PREFIX prov: <http://www.w3.org/ns/prov#>\n" + ";\n".join(per_snapshot_portions)

        return final_query_str



class DateTimeFixerFile:

    def __init__(self):
        pass

    def detect(graph:Graph) -> Optional[List[Tuple[URIRef]]]:

        result = []
        pattern_dt = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:(?:\+00:00)|Z)$"

        for s, p, o in graph.triples((None, None, None)):
            if p in (PROV.generatedAtTime, PROV.invalidatedAtTime):
                if not re.match(pattern_dt, str(o)):
                    result.append((graph.identifier, s, p, o))
        return result
    
    def fix_local_graph(ds:Dataset, graph:Graph, to_fix:tuple) -> None:

        for g_uri, subj, prop, obj in to_fix:
            correct_dt_res = Literal(normalise_datetime(str(obj)), datatype=XSD.dateTime)
            # ds.set((subj, prop, correct_dt_res))
            ds.remove((subj, prop, obj, g_uri))
            ds.add((subj, prop, correct_dt_res, g_uri))
    
    def build_update_query(to_fix:List[Tuple[URIRef]]):
        template = Template('''
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        DELETE DATA {
        $to_delete
        } ;
        INSERT DATA {
        $to_insert
        }
        ''')

        to_delete = []
        to_insert = []
        for g, s, p, dt in list(to_fix):
            g = str(g)
            s = str(s)
            p = str(p)
            dt = str(dt)

            to_delete.append(f'GRAPH <{g}> {{ <{s}> <{p}> "{dt}"^^xsd:dateTime . }}\n')
            correct_dt = normalise_datetime(dt)
            to_insert.append(f'GRAPH <{g}> {{ <{s}> <{p}> "{correct_dt}"^^xsd:dateTime . }}\n')
        
        to_delete_str = "\n  ".join(to_delete)
        to_insert_str = "\n  ".join(to_insert)
        query = template.substitute(to_delete=to_delete_str, to_insert=to_insert_str)
        return query


class MissingPrimSourceFixerFile:
    
    def __init__(self, meta_dumps_pub_dates):
        self.meta_dumps = meta_dumps_pub_dates

    def detect(graph:Graph) -> Optional[Tuple[URIRef, Literal]]:

        creation_se_uri = URIRef(graph.identifier + 'se/1')
        if ((creation_se_uri, PROV.generatedAtTime, None) in graph) and not ((creation_se_uri, PROV.hadPrimarySource, None) in graph):
            try:
                genTime = min(graph.objects(creation_se_uri, PROV.generatedAtTime))
            except ValueError:
                logging.warning(f"Could not find generatedAtTime value for creation snapshot {creation_se_uri}. Skipping MissingPrimSourceFixerFile detection.")
                return None
            return (creation_se_uri, genTime)

    def fix_local_graph(ds:Dataset, graph:Graph, to_fix:tuple, meta_dumps) -> None:
        
        primSource_str = get_previous_meta_dump_uri(meta_dumps, str(to_fix[1]))
        ds.add((to_fix[0], PROV.hadPrimarySource, URIRef(primSource_str), graph.identifier))
    

    def build_update_query(to_fix:List[Tuple[URIRef, Literal]], meta_dumps):

        template = Template("""
        PREFIX prov: <http://www.w3.org/ns/prov#>

        INSERT DATA {
            $quads
        }
        """)

        fixes = []
        for snapshot_uri, gen_time in to_fix:
            snapshot_uri = str(snapshot_uri)
            gen_time = str(gen_time)
            prim_source_uri = get_previous_meta_dump_uri(meta_dumps, gen_time)
            graph_uri = get_graph_uri_from_se_uri(snapshot_uri)
            fixes.append(f"GRAPH <{graph_uri}> {{ <{snapshot_uri}> prov:hadPrimarySource <{prim_source_uri}> . }}\n")
        quads_str = "    ".join(fixes)
        query = template.substitute(quads=quads_str)

        return query



class MultiPAFixerFile:

    def __init__(self):
        pass

    def detect(graph:Graph) -> Optional[List[Tuple[URIRef]]]:
        result = []
        for s, _, o in graph.triples((None, PROV.wasAttributedTo, None)):
            processing_agents = list(graph.objects(s, PROV.wasAttributedTo, unique=True))
            if len(processing_agents) > 1 and URIRef('https://w3id.org/oc/meta/prov/pa/1') in processing_agents:
                
                result.append((graph.identifier, s))
        return result
    

    def fix_local_graph(ds:Dataset, graph:Graph, to_fix:List[Tuple[URIRef]]) -> None:
        
        for g_uri, subj in to_fix:
            # ds.set((subj, PROV.wasAttributedTo, URIRef('https://w3id.org/oc/meta/prov/pa/2')))
            for obj in ds.objects(subj, PROV.wasAttributedTo, unique=True):
                ds.remove((subj, PROV.wasAttributedTo, obj, g_uri))
            ds.add((subj, PROV.wasAttributedTo, URIRef('https://w3id.org/oc/meta/prov/pa/2'), g_uri))
    

    def build_update_query(to_fix:List[Tuple[URIRef]]):

        template = Template("""
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

        DELETE DATA {
          $quads_to_delete
        } ;
        INSERT DATA {
          $quads_to_insert
        }
        """)
        
        to_delete = []
        to_insert = []
        for g, s in to_fix:
            g = str(g)
            s = str(s)
            to_delete.append(f"GRAPH <{g}> {{ <{s}> prov:wasAttributedTo <https://orcid.org/0000-0002-8420-0696> . }}\n")  # deletes Arcangelo's ORCID
            to_delete.append(f"GRAPH <{g}> {{ <{s}> prov:wasAttributedTo <https://w3id.org/oc/meta/prov/pa/1> . }}\n")  # deletes Meta's default processing agent (for ingestions only)
            to_insert.append(f"GRAPH <{g}> {{ <{s}> prov:wasAttributedTo <https://w3id.org/oc/meta/prov/pa/2> . }}\n")  # inserts Meta's processsing agent for modification processes

        to_delete_str = "  ".join(to_delete)
        to_insert_str = "  ".join(to_insert)
        query = template.substitute(quads_to_delete=to_delete_str, quads_to_insert=to_insert_str)
        
        return query
    

class MultiObjectFixerFile:

    def __init__(self):
        pass

    def detect(graph:Graph) -> Optional[Tuple[URIRef, Literal]]:

        creation_se_uri = URIRef(graph.identifier + 'se/1')

        for prop in {PROV.invalidatedAtTime, PROV.hadPrimarySource, URIRef('https://w3id.org/oc/ontology/hasUpdateQuery')}:
            for s in graph.subjects():
                if len(list(graph.objects(s, prop, unique=True))) > 1:
                    try:
                        creation_gen_time = min(graph.objects(creation_se_uri, PROV.generatedAtTime, unique=True))
                    except ValueError:
                        logging.error(f"Could not find generatedAtTime value for creation snapshot {creation_se_uri}. Skipping MultiObjectFixerFile detection.")
                        return None
                    return (graph.identifier, creation_gen_time)
    

    def fix_local_graph(ds:Dataset, graph:Graph, to_fix:tuple, meta_dumps) -> None:

        creation_se_uri = URIRef(graph.identifier + 'se/1')
        genTime_str = to_fix[1]
        primSource_str = get_previous_meta_dump_uri(meta_dumps, genTime_str)
        referent = URIRef(get_described_res_omid(str(creation_se_uri)))
        desc = Literal(f"The entity '{str(referent)}' has been created.")
        triples_to_add = (
            (creation_se_uri, PROV.hadPrimarySource, URIRef(primSource_str)),
            (creation_se_uri, PROV.wasAttributedTo, URIRef('https://w3id.org/oc/meta/prov/pa/1')),
            (creation_se_uri, PROV.specializationOf, referent),
            (creation_se_uri, DCTERMS.description, desc),
            (creation_se_uri, RDF.type, PROV.Entity),
            (creation_se_uri, PROV.generatedAtTime, Literal(genTime_str, datatype=XSD.dateTime))
        )

        ds.remove((None, None, None, graph.identifier))
        for t in triples_to_add:
            quad = t + (graph.identifier, )
            ds.add(quad)
    

    def build_update_query(to_fix, meta_dumps, pa_uri="https://w3id.org/oc/meta/prov/pa/1"):

        prefixes = """
        PREFIX prov: <http://www.w3.org/ns/prov#>
        PREFIX dcterms: <http://purl.org/dc/terms/>
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n\n
        """
        
        per_graph_template = Template("""
        CLEAR GRAPH <$graph> ;
        INSERT DATA {
          GRAPH <$graph> {
            <$creation_snapshot> prov:hadPrimarySource <$primary_source> ;
              prov:wasAttributedTo <$processing_agent> ;
              prov:specializationOf <$specialization_of> ;
              dcterms:description "$description" ;
              rdf:type prov:Entity ;
              prov:generatedAtTime "$gen_time"^^xsd:dateTime .
          }
        }
        """)

        query_parts = []
        for g, gen_time in to_fix:
            g = str(g)
            gen_time = str(gen_time)
            creation_se = g + 'se/1'
            gen_time = gen_time.replace("^^xsd:dateTime", "") 
            gen_time = gen_time.replace("^^http://www.w3.org/2001/XMLSchema#dateTime", "")
            prim_source = get_previous_meta_dump_uri(meta_dumps, gen_time)
            processing_agent = pa_uri 
            referent = get_described_res_omid(g)
            desc = f"The entity '{referent}' has been created."

            per_graph_part = per_graph_template.substitute(
                graph = g,
                creation_snapshot = creation_se,
                primary_source = prim_source, 
                processing_agent = processing_agent,
                specialization_of = referent, 
                description = desc,
                gen_time = gen_time
            )
            query_parts.append(per_graph_part)
        
        query = prefixes + " ; \n\n".join(query_parts)

        return query



def prepare_filler_issues(data_dir)->Tuple[List[tuple], int]:

    result = []
    tot_files = len(get_rdf_prov_filepaths(data_dir))

    for file_data in tqdm(
            read_rdf_dump(data_dir, whole_file=True),
            desc=f'Detecting graphs with fillers...',
            total=tot_files,
            dynamic_ncols=True
        ):
        stringified_data = json.dumps(file_data)
        d = Dataset(default_union=True)
        d.parse(data=stringified_data, format='json-ld')

        for graph in d.graphs():
            issues_in_graph = FillerFixerFile.detect(graph)
            if issues_in_graph:
                result.append(issues_in_graph)
    return result, tot_files



def sparql_update(
    client: SPARQLClient,
    update_query: str,
    failed_log_fp: str,
) -> bool:
    """
    Execute a SPARQL UPDATE via client.update().

    Uses the client's built-in retry settings. If the update still fails
    after all retries, writes the query to `failed_log`.

    Returns:
        True if the update succeeded, False if it failed and was logged.
    """
    try:
        client.update(update_query)
        return True  # success

    except QueryError as exc:
        # log syntax errors that weren't recoverable
        msg = str(exc)[:1000].replace('\n', '\\n ')
        logging.warning(f"SPARQL UPDATE failed after retries: {type(exc).__name__}: {msg}...")
        with open(failed_log_fp, 'a', encoding='utf-8') as lf:
            lf.write(update_query.replace("\n", "\\n") + "\n")
            lf.write(f"# Failure: {type(exc).__name__}: {exc}\n\n")
            lf.flush()
        return False

    except EndpointError as e:
        # log endpoint errors that weren't recoverable
        msg = str(e)[:1000].replace('\n', '\\n ')
        logging.warning(f"SPARQL UPDATE failed after retries: {type(e).__name__}: {msg}...")
        with open(failed_log_fp, 'a', encoding='utf-8') as lf:
            lf.write(update_query.replace("\n", "\\n") + "\n")
            lf.write(f"# Failure: {type(e).__name__}: {e}\n\n")
            lf.flush()
            
        logging.warning(f"EndpointError --> Possible DB warmup underway: sleeping 15 minutes before sending other udpates...")
        time.sleep(900)      

        return False
    
    finally:
        time.sleep(0.1)  # brief pause to avoid overwhelming the endpoint with rapid retries or subsequent updates


def fix_provenance_process(
        endpoint,
        data_dir,
        out_dir,
        meta_dumps_register,
        dry_run_db=False,
        dry_run_files=False,
        dry_run_callback=None,
        chunk_size=100,
        failed_queries_fp=f"prov_fix_failed_queries_{datetime.today().strftime('%Y-%m-%d')}.txt",
        overwrite_ok=False,
        resume=True,
        checkpoint_fp="fix_prov.checkpoint.json",
        cache_fp="filler_issues.cache.json",
        client_recreate_interval=100,
        zip_output=True,
    ):
    """
    Fix OpenCitations Meta provenance issues found in RDF dump files and optionally apply fixes to a
    SPARQL endpoint and output files.

    This function processes all provenance JSON-LD files in ``data_dir`` and detects common
    issues: fillers, invalid datetime formats, missing primary sources, multiple processing
    agents, and multiple object occurrences. It applies fixes locally to an ``rdflib.Dataset`` 
    corresponding to each file and, unless ``dry_run_db`` is ``True``, issues SPARQL UPDATE 
    requests to ``endpoint``. If ``dry_run_files`` is ``False``, fixed datasets are dumped to 
    a file in a subdirectory of ``out_dir`` with a path derived from the input file path 
    relative to ``data_dir`` (else, no output files are written).

    :param str endpoint: URL of the SPARQL endpoint used to apply updates.
    :param str data_dir: Path to the directory containing provenance JSON-LD dump files to
        process.
    :param str out_dir: Path where fixed JSON-LD files will be written.
    :param Iterable[Tuple[str, str]] meta_dumps_register: Iterable of ``(publication_date_iso,
        dump_url)`` pairs used to compute primary source URIs.
    :param bool dry_run_db: If ``True``, no SPARQL updates are sent to the endpoint.
        Defaults to ``False``.
    :param bool dry_run_files: If ``True``, no output files are written to ``out_dir``.
        Defaults to ``False``.
    :param callable dry_run_callback: Callback invoked when ``dry_run_db`` is ``True`` with
        signature ``(file_path, (ff_issues, dt_issues, mps_issues, pa_issues, mo_issues))``.
    :param int chunk_size: Number of items per SPARQL update batch. Defaults to ``100``.
    :param str failed_queries_fp: Path to a log file where failing SPARQL queries are appended.
        Defaults to ``prov_fix_failed_queries_YYYY-MM-DD.txt``.
    :param bool overwrite_ok: When ``False``, a :class:`FileExistsError` is raised if a target
        output file already exists. Defaults to ``False``.
    :param bool resume: When ``True``, use the checkpoint file to skip already-processed files
        and steps. Defaults to ``True``.
    :param str checkpoint_fp: Path of the checkpoint JSON file used to record progress for
        resuming.
    :param str cache_fp: Path to the filler issues cache used to avoid re-scanning ``data_dir``.
    :param int client_recreate_interval: Number of files to process before recreating the SPARQLClient
        to prevent pycurl's accumulated state from degrading performance. Defaults to ``100``.
        This is necessary because pycurl's Curl object accumulates internal state (DNS cache,
        connection pool, SSL/TLS session state) over hundreds of thousands of requests,
        causing progressive performance degradation.
    :param bool zip_output: If ``True``, output files are compressed using zip. Defaults to ``True``.

    :returns: None
    :rtype: None

    :raises FileExistsError: If ``overwrite_ok`` is False and an output file already exists.
    :raises RuntimeError: If the function would write inside ``data_dir`` (safeguard to avoid
        corrupting input).
    :raises Exception: Other exceptions may be raised for unexpected errors or endpoint
        failures.

    Side effects
    - Writes fixed JSON-LD files into ``out_dir`` (unless ``dry_run``).
    - May send SPARQL UPDATE requests to ``endpoint`` (unless ``dry_run``).
    - Creates or updates ``checkpoint_fp`` and ``cache_fp`` files.
    - Logs summary information and error details.
    """

    start_time = time.time()

    os.makedirs(out_dir, exist_ok=True)
    logging.info(f"[Provenance fixing process paradata]: {locals()}") # log parameters

    checkpoint = Checkpoint(checkpoint_fp)
    client = SPARQLClient(endpoint)
    ff_c, dt_c, mps_c, pa_c, mo_c = 0, 0, 0, 0, 0  # counters for issues
    client_reset_counter = 0  # Track files processed with current client instance
    times_per_file = []

    try:
        logging.info("Provenance fixing process started.")
        logging.info(f"Checkpoint state: {str(checkpoint.state)}")
        logging.info("Detecting Filler issues (via cache or new scan)...")
        # check cache for filler issues or get them
        filler_issues, tot_files, filler_cache_fp = load_or_prepare_filler_issues(data_dir, cache_fp) 
        rename_mapping = FillerFixerFile.make_global_rename_map(filler_issues)
        graphs_with_fillers = {t[0]: t[1] for t in filler_issues}

        meta_dumps = sorted(
            [(date.fromisoformat(d), url) for d, url in meta_dumps_register],
            key=lambda x: x[0]
        )
        
        logging.info("Processing RDF dump files...")
        for file_index, (file_data, fp) in enumerate(
                tqdm(
                    read_rdf_dump(data_dir, whole_file=True, include_fp=True), 
                    desc="Processing RDF dump files...",
                    total=tot_files,
                    dynamic_ncols=True
                )
            ):

            start_file = time.time()

            if resume and checkpoint.should_skip_file(file_index):
                continue

            stringified_data = json.dumps(file_data)
            d = Dataset(default_union=True)
            d.parse(data=stringified_data, format='json-ld')

            ff_issues_in_file = []
            dt_issues = []
            mps_issues = []
            pa_issues = []
            mo_issues = []

            # ---------------- FILLER FIXER ----------------
            if not (resume and checkpoint.step_completed(Step.FILLER, file_index)):

                for graph in d.graphs():
                    if graph.identifier == d.default_graph.identifier:
                        continue
                    ff_to_fix_val = graphs_with_fillers.get(graph.identifier)
                    if ff_to_fix_val:
                        ff_issues_in_file.append((graph.identifier, ff_to_fix_val))
                        ff_c += 1
                        FillerFixerFile.fix_local_graph(d, graph, rename_mapping, graphs_with_fillers)

                for chunk in batched(ff_issues_in_file, chunk_size):
                    for t in chunk:
                        g_id = str(t[0])
                        to_delete = [str(i) for i in t[1]['to_delete']]
                        to_rename = [str(i) for i in t[1]['remaining_snapshots']]
                        local_mapping = FillerFixerFile.map_se_names(to_delete, to_rename)
                        newest_names = list(set(local_mapping.values()))

                        if not dry_run_db:
                            sparql_update(client,
                                        FillerFixerFile.build_delete_sparql_query(t),
                                        failed_queries_fp)
                            sparql_update(client,
                                        FillerFixerFile.build_rename_sparql_query(local_mapping),
                                        failed_queries_fp)
                            sparql_update(client,
                                        FillerFixerFile.build_adapt_invaltime_sparql_query(g_id, newest_names),
                                        failed_queries_fp)

                checkpoint.update_state(file_index, fp, Step.FILLER, endpoint_done=True, local_done=False)

            # ---------------- DATETIME FIXER ----------------
            if not (resume and checkpoint.step_completed(Step.DATETIME, file_index)):

                for graph in d.graphs():
                    if graph.identifier != d.default_graph.identifier:
                        issues = DateTimeFixerFile.detect(graph)
                        if issues:
                            dt_issues.extend(issues)
                            dt_c += len(issues)
                            DateTimeFixerFile.fix_local_graph(d, graph, issues)

                if not dry_run_db:
                    for chunk in batched(dt_issues, chunk_size):
                        sparql_update(client,
                                    DateTimeFixerFile.build_update_query(chunk),
                                    failed_queries_fp)

                checkpoint.update_state(file_index, fp, Step.DATETIME, endpoint_done=True, local_done=False)

            # ---------------- MISSING PRIMARY SOURCE ----------------
            if not (resume and checkpoint.step_completed(Step.MISSING_PS, file_index)):

                for graph in d.graphs():
                    if graph.identifier != d.default_graph.identifier:
                        issue = MissingPrimSourceFixerFile.detect(graph)
                        if issue:
                            mps_issues.append(issue)
                            mps_c += 1
                            MissingPrimSourceFixerFile.fix_local_graph(d, graph, issue, meta_dumps)

                if not dry_run_db:
                    for chunk in batched(mps_issues, chunk_size):
                        sparql_update(client,
                                    MissingPrimSourceFixerFile.build_update_query(chunk, meta_dumps),
                                    failed_queries_fp)

                checkpoint.update_state(file_index, fp, Step.MISSING_PS, endpoint_done=True, local_done=False)

            # ---------------- MULTI PA FIXER ----------------
            if not (resume and checkpoint.step_completed(Step.MULTI_PA, file_index)):

                for graph in d.graphs():
                    if graph.identifier != d.default_graph.identifier:
                        issues = MultiPAFixerFile.detect(graph)
                        if issues:
                            pa_issues.extend(issues)
                            pa_c += len(issues)
                            MultiPAFixerFile.fix_local_graph(d, graph, issues)

                if not dry_run_db:
                    for chunk in batched(pa_issues, chunk_size):
                        sparql_update(client,
                                    MultiPAFixerFile.build_update_query(chunk),
                                    failed_queries_fp)

                checkpoint.update_state(file_index, fp, Step.MULTI_PA, endpoint_done=True, local_done=False)

            # ---------------- MULTI OBJECT FIXER ----------------
            if not (resume and checkpoint.step_completed(Step.MULTI_OBJECT, file_index)):

                for graph in d.graphs():
                    if graph.identifier != d.default_graph.identifier:
                        issue = MultiObjectFixerFile.detect(graph)
                        if issue:
                            mo_issues.append(issue)
                            mo_c += 1
                            MultiObjectFixerFile.fix_local_graph(d, graph, issue, meta_dumps)

                if not dry_run_db:
                    for chunk in batched(mo_issues, chunk_size):
                        sparql_update(client,
                                    MultiObjectFixerFile.build_update_query(chunk, meta_dumps),
                                    failed_queries_fp)

                checkpoint.update_state(file_index, fp, Step.MULTI_OBJECT, endpoint_done=True, local_done=False)

            # ---------------- WRITE OUTPUT (FIXED) FILE ----------------
            if not (resume and checkpoint.step_completed(Step.WRITE_FILE, file_index)):
                
                if not dry_run_files:
                    abs_data_dir = Path(data_dir).resolve()
                    abs_out_dir = Path(out_dir).resolve()

                    rel_path = Path(fp).resolve().relative_to(abs_data_dir)
                    fixed_fp = abs_out_dir / rel_path
                    fixed_fp = fixed_fp.with_suffix('.json')

                    fixed_fp.parent.mkdir(parents=True, exist_ok=True)

                    if os.path.isfile(fixed_fp) and not overwrite_ok:
                        raise FileExistsError(f"{fixed_fp} already exists")
                    
                    if abs_data_dir in fixed_fp.parents:  # safeguard for not corrupting input data
                        raise RuntimeError(f"Refusing to write inside data_dir! {fixed_fp}")

                    out_data = d.serialize(format='json-ld', indent=None, separators=(', ', ': '))

                    if zip_output:
                        with ZipFile(fixed_fp.with_suffix('.zip'), 'w', compression=ZIP_DEFLATED, allowZip64=True) as zipf:
                            zipf.writestr(fixed_fp.name, out_data)
                    else:
                        with open(fixed_fp, 'w', encoding='utf-8') as out_file:
                            out_file.write(out_data)
            
                checkpoint.update_state(file_index, fp, Step.WRITE_FILE, endpoint_done=True, local_done=True)

            checkpoint.flush()

            # Periodically recreate SPARQLClient to prevent pycurl's accumulated state degradation
            client_reset_counter += 1
            if not dry_run_db and client_reset_counter >= client_recreate_interval:
                logging.debug(f"Recreating SPARQLClient after {client_reset_counter} files to clear accumulated pycurl state")
                client.close()
                client = SPARQLClient(endpoint)
                client_reset_counter = 0

            if dry_run_db and dry_run_callback:  # use callback function to use issues found in each file
                dry_run_callback(fp, (ff_issues_in_file, dt_issues, mps_issues, pa_issues, mo_issues))
            
            elapsed_file :float = time.time() - start_file
            times_per_file.append(elapsed_file)
            if file_index % 500 == 0:
                avg_time = sum(times_per_file)/len(times_per_file)
                est_remaining = avg_time * (tot_files - file_index - 1)
                logging.info(f"Average time per file with last {len(times_per_file)} files: {avg_time:.2f} seconds. Estimated remaining time: {est_remaining/3600:.2f} hours.")
                times_per_file = []


        # successful termination -> cleanup
        elapsed = time.time() - start_time
        logging.info(f"Provenance fixing process completed successfully in {elapsed/3600:.2f} hours.")
        print(f"Provenance fixing process completed successfully in {elapsed/3600:.2f} hours.")
        logging.info(f"Total Filler issues found and fixed: {ff_c}")
        logging.info(f"Total DateTime issues found and fixed: {dt_c}")
        logging.info(f"Total Missing Primary Source issues found and fixed: {mps_c}")
        logging.info(f"Total Multiple Processing Agent issues found and fixed: {pa_c}")
        logging.info(f"Total Multiple Object issues found and fixed: {mo_c}")
        
        if os.path.exists(filler_cache_fp):
            os.remove(filler_cache_fp)
        if os.path.exists(checkpoint.path):
            os.remove(checkpoint.path)

    except (Exception, KeyboardInterrupt) as e:
        print(traceback.print_exc())
        if type(e) == KeyboardInterrupt:
            logging.error("KeyboardInterrupt")
        else:
            logging.error(e)
        elapsed = time.time() - start_time
        logging.info(f"Process ran for {elapsed/3600:.2f} hours before interruption.")
        logging.info(f"Checkpoint state at process interruption: {checkpoint.state}")

    finally:
        checkpoint.flush()
        client.close()
