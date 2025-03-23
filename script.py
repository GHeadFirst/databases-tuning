import sys
from straightforward_implementation import run_straightforward
from efficient_copy import run_copy
from efficient_batch import run_batch

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py [method]")
        print("Methods: straightforward, copy, batch")
        sys.exit(1)

    method = sys.argv[1]

    if method == "straightforward":
        run_straightforward()
    elif method == "copy":
        run_copy()
    elif method == "batch":
        run_batch()
    else:
        print(f"Unknown method: {method}")
