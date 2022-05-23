import numpy as np
from pathlib import Path
from typing import List
import ray

dt = np.dtype(np.uint32)

def generate_file(numbers_count:int, output_path:Path) -> None:   
    res = np.random.randint(0, 2**32, numbers_count, dtype=dt)
    res = list(map(lambda x: str(x) + '\n', res))
    res[-1] = res[-1].rstrip()
    with open(output_path, 'w') as f:
        f.writelines(res)


def read_file(input_path:Path):
    res = list(open(input_path))
    res = np.array(res, dtype=dt)
    return res


def factorize(num:np.uint32):
    num_ = num.copy()
    prime_factors = []
    while num_ % 2 == 0:
        prime_factors.append(2)
        num_ /= 2

    for i in range(3, int(np.sqrt(num_)) + 1, 2):
        while num_ % i == 0:
            prime_factors.append(int(i))
            num_ /= i
    
    if num_ > 2:
        prime_factors.append(int(num_))
    
    return prime_factors


def factorize_parallel(num_list:List[np.uint32]):
    sum = 0
    for num in num_list:
        res = factorize(num)
        sum += len(res)
    return sum

        


if __name__ == '__main__':
    filename = Path('./numbers.txt')
    generate_file(2000, filename)
    num_list = read_file(filename)
    print(factorize(num_list[5]))