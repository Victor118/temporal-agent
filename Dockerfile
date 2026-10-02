FROM golang:1.24

WORKDIR /app

RUN git config --system --add safe.directory /app

# The user exec runs its commands as. The worker runs as root, and a command
# run as root would read the platform's credentials back from the worker's
# /proc/<pid>/environ, whatever its own environment holds. The worker refuses
# exec as root without RUN_AS_UID, and gives this user the workspace at
# startup.
RUN groupadd --gid 10001 agent-run \
 && useradd --uid 10001 --gid 10001 --no-log-init --create-home --home-dir /home/agent-run \
      --shell /usr/sbin/nologin agent-run
ENV RUN_AS_UID=10001

CMD ["sh", "-c", "rm -f ./tmp/main && go build -buildvcs=false -o ./tmp/main ./cmd/agent && exec ./tmp/main dev"]
