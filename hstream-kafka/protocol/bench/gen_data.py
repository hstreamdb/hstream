from kafka.record import MemoryRecordsBuilder
import random


# magic: [0, 1, 2]
def encode_records(magic, key, value, n):
    # compression_type: [0, 1, 2, 3]
    builder = MemoryRecordsBuilder(
        magic=magic, compression_type=0, batch_size=len(value) * n
    )
    for offset in range(n):
        builder.append(timestamp=10000 + offset, key=key, value=value)
    builder.close()
    return builder.buffer()


def write_records(file_name, bs):
    with open(file_name, "wb") as f:
        f.write(bs)


if __name__ == "__main__":
    write_records(
        "/tmp/records_1k_1.data",
        encode_records(2, None, random.randbytes(1024), 1),
    )
    write_records(
        "/tmp/records_1k_100.data",
        encode_records(2, None, random.randbytes(1024), 100),
    )
    write_records(
        "/tmp/records_1k_1000.data",
        encode_records(2, None, random.randbytes(1024), 1000),
    )
