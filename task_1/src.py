import numpy as np
from pathlib import Path
import mmap


def generate_file(size: int, write_mode:str = 'open', output_path: Path = Path('./output.npy')) -> None:
    size_in_bits = size * 8 * (1024**3)
    dt = np.dtype(np.uint32).newbyteorder('big')
    res = np.random.randint(0, 2**32, size_in_bits // 32, dtype=dt)
    if write_mode == 'np.load':
        np.save(output_path, res)
    if write_mode == 'open':
        with open(output_path, 'wb') as f:
            f.write(res.data)
            
   
def read_mm_file(output_path: Path = Path('./output.npy')) -> np.ndarray:
    with open(output_path, 'r+b') as f:
        buf = mmap.mmap(f.fileno(), length=0, offset=0,  access=mmap.ACCESS_READ)
        dt = np.dtype(np.uint32).newbyteorder('big')
        res = np.frombuffer(buf, dtype=dt)
    return res
        
        
def read_file(input_path: Path = Path('./output.npy'), mode:str = 'open') -> np.ndarray:
    dt = np.dtype(np.uint32).newbyteorder('big')
    if mode == 'open':
        with open(input_path, 'r+b') as f:
            res = np.frombuffer(f.read(), dtype=dt)
        return res
    if mode == 'np.load':       
        res = np.load(input_path)
    return res


if __name__ == '__main__':
    generate_file(2)
