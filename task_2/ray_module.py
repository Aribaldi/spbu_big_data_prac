import ray
from src import read_file, factorize_parallel
import numpy as np
from pathlib import Path
from src import factorize_parallel
import timeit
import argparse

parser = argparse.ArgumentParser(description='Prime factors counting using ray mp')
parser.add_argument('--n_iters', type=int, default=7, help='Number of timeit iterations')

@ray.remote
def map_f(obj, f):
    return f(obj)


@ray.remote
def sum_counts(*counts):
    return np.sum(counts)


def main_wrapper(split):
    factors_counts = [map_f.remote(i, factorize_parallel) for i in split]
    final_sum = sum_counts.remote(*factors_counts)
    return ray.get(final_sum)



if __name__ == '__main__':
    arg = parser.parse_args()
    n_iters = vars(arg)['n_iters']
    ray.init(num_cpus=4)
    num_list = read_file(Path('./numbers.txt'))
    data_split = np.array_split(np.array(num_list), 4)
    res = main_wrapper(data_split)
    print(f'Result:{res}')
    mean_time = timeit.timeit('main_wrapper(data_split)', 
                        number=7, 
                        globals=globals()) / n_iters 
    
    print(f'Mean elapsed time (from {n_iters} runs): {mean_time // 1:.0f}s {mean_time % 1 * 1e3:.0f}ms')