import time
import threading
import logging
import docker
import traceback
from SPARQLWrapper import SPARQLWrapper, JSON
from sparqlite import SPARQLClient


def wait_for_sparql(endpoint: str, timeout: int = 120) -> bool:
    """Wait until SPARQL endpoint responds to a simple ASK query."""
    sparql = SPARQLWrapper(endpoint)
    sparql.setQuery("ASK {}")
    sparql.setReturnFormat(JSON)

    start = time.time()
    while time.time() - start < timeout:
        try:
            sparql.query()
            return True
        except Exception:
            time.sleep(2)
    return False


def sparql_healthcheck(endpoint: str, timeout: int = 5) -> bool:
    """Run a lightweight SPARQL SELECT healthcheck."""
    q = """
        SELECT ?s ?p ?o
        WHERE { ?s ?p ?o }
        LIMIT 1
    """

    try:
        with SPARQLClient(endpoint) as client:
            result = client.query(q)
            if len(result["results"]["bindings"]) > 0:
                return True
            else:
                return False
    except Exception:
        return False


def monitor_and_restart(
    container_name: str,
    endpoint: str,
    threshold: float = 0.98,
    restart_interval: int = 10800, # 3 hours
    mem_check_interval: int = 3600,
    healthcheck_interval: int = 180,
):
    """
    Background watchdog thread.

    - Monitors Docker container memory usage
    - Runs SPARQL healthchecks periodically
    - Restarts container if memory usage exceeds threshold
      or if SPARQL healthcheck fails consecutively
    
    :param container_name: Name of the Docker container running Virtuoso
    :param endpoint: SPARQL endpoint URL
    :param threshold: Memory usage threshold (fraction of limit) to trigger restart
    :param restart_interval: Interval (seconds) between restarts, to force periodic restarts even if memory usage is below threshold (default 3 hours)
    :param mem_check_interval: Interval (seconds) between memory usage checks
    :param healthcheck_interval: Interval (seconds) between SPARQL healthchecks
    """
    client = docker.from_env()
    GiB = 1024 ** 3

    last_restart = 0
    last_mem_check = 0
    last_healthcheck = 0

    while True:
        now = time.time()

        try:
            container = client.containers.get(container_name)

            if now - last_restart > restart_interval:
                logging.warning(f"[Virtuoso watchdog] Restart interval exceeded ({last_restart/3600} hours)-> restarting container")
                container.restart()
                last_restart = now

                logging.info("[Virtuoso watchdog] Sleeping 15 minutes to allow Virtuoso to restart and stabilize before healthcheck...")
                time.sleep(900)
                    
                if sparql_healthcheck(endpoint):
                    logging.info("[Virtuoso watchdog] SPARQL endpoint is back online")
                else:
                    logging.error("[Virtuoso watchdog] SPARQL endpoint DID NOT recover within timeout!")

            # # --- Virtuoso healthcheck ---
            # if now - last_healthcheck >= healthcheck_interval:
            #     last_healthcheck = now

            #     if not sparql_healthcheck(endpoint):
            #         logging.error(
            #             "[Virtuoso watchdog] SPARQL healthcheck failed -> restarting container"
            #         )
            #         container.restart()

            #         logging.info("[Virtuoso watchdog] Waiting for SPARQL endpoint to recover…")
            #         if wait_for_sparql(endpoint):
            #             logging.info("[Virtuoso watchdog] SPARQL endpoint is back online")
            #         else:
            #             logging.error("[Virtuoso watchdog] SPARQL endpoint DID NOT recover within timeout!")


            # --- Container memory usage check ---
            if now - last_mem_check >= mem_check_interval:
                last_mem_check = now

                stats = container.stats(stream=False)

                used = stats["memory_stats"]["usage"]
                limit = stats["memory_stats"]["limit"]
                cache = stats["memory_stats"]["stats"].get("inactive_file", 0)
                effective_used = used - cache
                ratio = effective_used / limit

                logging.info(
                    f"[Virtuoso watchdog] Mem use: "
                    f"{effective_used/GiB:.2f}GiB / {limit/GiB:.2f}GiB "
                    f"({ratio*100:.1f}%)"
                )

                if ratio > threshold:
                    logging.warning(
                        "[Virtuoso watchdog] Memory above threshold -> restarting container"
                    )
                    container.restart()

                    logging.info("[Virtuoso watchdog] Waiting for SPARQL endpoint to recover…")
                    logging.info("Sleeping 15 minutes to allow Virtuoso to restart and stabilize before healthcheck...")
                    time.sleep(900) 
                    
                    if wait_for_sparql(endpoint):
                        logging.info("[Virtuoso watchdog] SPARQL endpoint is back online")
                    else:
                        logging.error("[Virtuoso watchdog] SPARQL endpoint DID NOT recover within timeout!")

        except Exception:
            logging.error("[Virtuoso watchdog] Unexpected error", exc_info=True)

        # Small sleep to avoid busy-waiting
        time.sleep(5)


def start_watchdog_thread(container_name: str, endpoint: str):
    t = threading.Thread(
        target=monitor_and_restart,
        args=(container_name, endpoint),
        daemon=True,
    )
    t.start()
