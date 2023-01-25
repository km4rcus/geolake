set -x
#
# backup geokube-dds DB
#
echo "[$(date)] [info] Start geokube-dds DB backup"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
FILENAME=/snapshots/${POSTGRES_DB}-${TIMESTAMP}.bak
echo "[$(date)] [info] Dumping geokube-dds DB in ${FILENAME}"
PGPASSWORD=${POSTGRES_PASSWORD} pg_dump -U ${POSTGRES_USER} -h ${POSTGRES_HOST} -Fc -f ${FILENAME} ${POSTGRES_DB}