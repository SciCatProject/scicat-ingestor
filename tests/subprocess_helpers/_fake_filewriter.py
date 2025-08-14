def fake_file_writer(test_file_name: str, writing_done_delay: int = 5) -> None:
    import time

    import h5py

    with h5py.File(test_file_name, "w") as f:
        entry = f.create_group('entry')
        entry.create_dataset('title', data=['Test Title'])

        time.sleep(writing_done_delay)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fake file writer for testing.")
    parser.add_argument(
        "test_file_name", type=str, help="The name of the test file to write."
    )
    parser.add_argument(
        "--delay",
        type=int,
        default=5,
        help="Delay before finishing the write (in seconds).",
    )
    args = parser.parse_args()

    fake_file_writer(args.test_file_name, args.delay)
