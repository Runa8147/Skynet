"""
scheduler.py  -  GA-based frame scheduler for Orion Render Cloud

Accepts an explicit list of frame numbers so non-contiguous recovery sets
work correctly. Reads historical fps_history from cloud_state.json to seed
scores from past runs.
"""

import random
import json
import os


def load_history_from_file(path="cloud_state.json"):
    """Return {worker_name: avg_fps} from saved state file."""
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            saved = json.load(f)
        result = {}
        for wname, wdata in saved.get("workers", {}).items():
            if wname == "master":
                continue
            fps_list = wdata.get("fps_history", [])
            if fps_list:
                result[wname] = sum(fps_list) / len(fps_list)
            elif "score" in wdata:
                result[wname] = float(wdata["score"])
        return result
    except Exception:
        return {}


def generate_frame_schedule(
    workers,
    history,
    frames,
    pop_size=30,
    generations=60,
    state_file="cloud_state.json",
):
    """
    Parameters
    ----------
    workers     : list of worker names (should NOT include 'master')
    history     : {worker_name: fps_score}  higher = faster
    frames      : list of int frame numbers to distribute
    pop_size    : GA population size
    generations : GA iterations

    Returns
    -------
    {worker_name: {"frames": [list_of_frame_numbers]}}
    - Every worker receives at least 1 frame.
    - Every frame in `frames` appears in exactly one worker's list.
    - No frame is duplicated.
    """
    # Exclude master from scheduling
    workers = [w for w in workers if w != "master"]

    if not workers:
        return {}
    if not frames:
        return {}

    frames = sorted(frames)   # canonical order
    n      = len(workers)
    total  = len(frames)

    # Merge runtime history with saved file history
    file_hist = load_history_from_file(state_file)
    merged = {}
    for w in workers:
        vals = []
        if w in history:
            vals.append(float(history[w]))
        if w in file_hist:
            vals.append(float(file_hist[w]))
        merged[w] = (sum(vals) / len(vals)) if vals else 1.0

    scores    = [max(merged[w], 1e-9) for w in workers]
    total_sc  = sum(scores)

    if n == 1:
        return {workers[0]: {"frames": frames}}

    # ── proportional baseline ─────────────────────────────────────────────────
    def proportional_counts():
        raw    = [s / total_sc * total for s in scores]
        counts = [max(1, int(r)) for r in raw]
        diff   = total - sum(counts)
        if diff > 0:
            order = sorted(range(n), key=lambda i: raw[i] - int(raw[i]), reverse=True)
            for i in order:
                if diff == 0:
                    break
                counts[i] += 1
                diff -= 1
        elif diff < 0:
            order = sorted(range(n), key=lambda i: raw[i] - int(raw[i]))
            for i in order:
                if diff == 0:
                    break
                if counts[i] > 1:
                    counts[i] -= 1
                    diff += 1
        # last resort: dump remainder on fastest worker
        if sum(counts) != total:
            counts[max(range(n), key=lambda i: scores[i])] += total - sum(counts)
        return counts

    # ── repair: sum==total, every entry >= 1 ─────────────────────────────────
    def repair(counts):
        counts = [max(1, c) for c in counts]
        diff   = total - sum(counts)
        if diff == 0:
            return counts
        fastest = sorted(range(n), key=lambda i: scores[i], reverse=True)
        for i in fastest:
            if diff == 0:
                break
            if diff > 0:
                counts[i] += 1
                diff -= 1
            elif counts[i] > 1:
                counts[i] -= 1
                diff += 1
        if diff != 0:
            counts[fastest[0]] = max(1, counts[fastest[0]] + diff)
        return counts

    # ── fitness: minimise max wall-clock time across workers ─────────────────
    def fitness(counts):
        times = [counts[i] / scores[i] for i in range(n)]
        return -max(times)   # maximise == minimise bottleneck

    # ── GA operators ─────────────────────────────────────────────────────────
    def random_individual():
        base = proportional_counts()[:]
        for _ in range(max(1, n // 2)):
            donors = [i for i in range(n) if base[i] > 1]
            if not donors:
                break
            i = random.choice(donors)
            j = random.choice([k for k in range(n) if k != i])
            base[i] -= 1
            base[j] += 1
        return repair(base)

    def tournament(pop, k=3):
        sample = random.sample(pop, min(k, len(pop)))
        return max(sample, key=fitness)

    def crossover(p1, p2):
        pt    = random.randint(1, n - 1)
        child = p1[:pt] + p2[pt:]
        return repair(child)

    def mutate(ind):
        donors = [i for i in range(n) if ind[i] > 1]
        if not donors:
            return
        i = random.choice(donors)
        j = random.choice([k for k in range(n) if k != i])
        ind[i] -= 1
        ind[j] += 1

    # ── evolve ────────────────────────────────────────────────────────────────
    population = [random_individual() for _ in range(pop_size)]

    for _ in range(generations):
        new_pop = [max(population, key=fitness)[:]]   # elitism
        while len(new_pop) < pop_size:
            child = crossover(tournament(population), tournament(population))
            if random.random() < 0.25:
                mutate(child)
            new_pop.append(child)
        population = new_pop

    best = max(population, key=fitness)

    # Verify sum is correct before building schedule
    best = repair(best)

    # ── map counts -> actual frame numbers ────────────────────────────────────
    schedule = {}
    idx = 0
    for w, count in zip(workers, best):
        if count <= 0:
            continue
        schedule[w] = {"frames": frames[idx: idx + count]}
        idx += count

    # Assign any leftover (rounding edge case) to the fastest worker
    if idx < total:
        fastest_w = max(workers, key=lambda w: merged[w])
        schedule.setdefault(fastest_w, {"frames": []})
        schedule[fastest_w]["frames"].extend(frames[idx:])

    # Final sanity: assert no frame is duplicated or missing
    assigned = [f for asgn in schedule.values() for f in asgn["frames"]]
    assert sorted(assigned) == frames, \
        f"Scheduler bug: assigned={sorted(assigned)} != expected={frames}"

    return schedule
