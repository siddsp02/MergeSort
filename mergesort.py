import concurrent.futures
import random
import time
from typing import TypeVar

# Threshold for which mergesort will switch to multiprocessing.
# Any array (in either the original or recursive) call of the
# function will be sorted using the regular method if the length
# is less than the following amount.

ARRAY_SIZE_CUTOFF = 250000

T = TypeVar("T")


def merge(left: list[T], right: list[T]) -> list[T]:
    """Merging subroutine."""
    ret = []
    llen, rlen, i, j = len(left), len(right), 0, 0
    while i < llen and j < rlen:
        if left[i] > right[j]:  # type: ignore
            ret.append(right[j])
            j += 1
        else:
            ret.append(left[i])
            i += 1
    # While there are elements to process from the right array,
    # add them to the end of the resulting array.
    while i < llen:
        ret.append(left[i])
        i += 1
    # While there are elements to process from the right array,
    # add them to the end of the resulting array.
    while j < rlen:
        ret.append(right[j])
        j += 1
    # Return the new merged array.
    return ret


def mergesort(arr: list[T]) -> list[T]:
    """Regular mergesort."""
    n = len(arr)
    mid = n // 2
    if n < 2:
        return arr[:]
    left = mergesort(arr[:mid])
    right = mergesort(arr[mid:])
    return merge(left, right)


def pmergesort(arr: list[T]) -> list[T]:
    """Parallelized version of mergesort."""
    n = len(arr)
    mid = n // 2
    if n < 2:
        return arr[:]
    if n >= ARRAY_SIZE_CUTOFF:
        with concurrent.futures.ProcessPoolExecutor() as executor:
            f1 = executor.submit(pmergesort, arr[:mid])
            f2 = executor.submit(pmergesort, arr[mid:])
            left, right = f1.result(), f2.result()
    else:
        # Fall back to regular mergesort if the array is small enough.
        left, right = mergesort(arr[:mid]), mergesort(arr[mid:])
    return merge(left, right)


# A sample array of 1,000,000 elements for testing.
if __name__ == "__main__":
    sample = random.choices(range(5_000_000), k=1_000_000)
    t0 = time.perf_counter()
    a_sorted = mergesort(sample)
    t1 = time.perf_counter()
    b_sorted = pmergesort(sample)
    t2 = time.perf_counter()
    print(f"Single process took {t1 - t0:.2f} seconds.")
    print(f"Parallel took {t2 - t1:.2f} seconds.")
    assert a_sorted == b_sorted  # Check that both return the same results.
    print("DONE.")
