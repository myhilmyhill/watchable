# syntax=docker/dockerfile:1
FROM debian:bookworm-slim AS build_tsselect
RUN apt update && apt install -y gcc make

ADD --link https://github.com/xtne6f/tsselect_gcc.git#r3 /tsselect_gcc
RUN cd /tsselect_gcc/src && make && make install

FROM debian:bookworm-slim
RUN apt update && apt install -y python3 systemd && rm -rf /var/lib/apt/lists/*

COPY --from=build_tsselect /usr/local/bin/tsselect /usr/local/bin/tsselect
COPY --link ./watchable.py /usr/local/bin/watchable.py

CMD ["python3", "/usr/local/bin/watchable.py"]
