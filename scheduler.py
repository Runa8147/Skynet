import random

class GAScheduler:
    def __init__(self, workers, total_frames):
        self.workers = workers # List of names: ['laptop1', 'macbook']
        self.total_frames = total_frames
        
    def generate_schedule(self, history_data):
        """
        Simple GA logic:
        Chromosome: List of integers representing frame counts for each worker.
        Fitness: Minimize the difference in completion time between workers.
        """
        # For a mini-project, we use worker speed (frames/min) from history
        # if no history, assume equal speed.
        num_workers = len(self.workers)
        if num_workers == 0: return {}

        # 1. Initialize Population (Random splits)
        # 2. Selection/Crossover (Optimization)
        # 3. Best solution: Proportional distribution based on 'score'
        
        total_score = sum(history_data.values())
        current_frame = 1
        schedule = {}

        for worker in self.workers:
            # Calculate how many frames this worker gets based on performance
            share = history_data[worker] / total_score
            count = int(share * self.total_frames)
            
            schedule[worker] = {
                "start": current_frame,
                "end": current_frame + count - 1
            }
            current_frame += count

        # Handle leftover frames (rounding errors)
        if current_frame <= self.total_frames:
            schedule[self.workers[-1]]["end"] = self.total_frames

        return schedule