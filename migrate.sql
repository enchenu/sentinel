-- 1) eliminar duplicados (mantiene el registro con mayor id)
DELETE FROM uplinks a
USING uplinks b
WHERE a.id < b.id
AND a.dev_eui = b.dev_eui
AND a.f_cnt = b.f_cnt
AND a.received_at = b.received_at;


-- 2) remover constraint anterior (si existe)
ALTER TABLE uplinks DROP CONSTRAINT IF EXISTS uplinks_unique;


-- 3) crear nueva constraint única
ALTER TABLE uplinks
ADD CONSTRAINT uplinks_unique UNIQUE (dev_eui, f_cnt, received_at);


-- 4) verificar duplicados (debe devolver 0 filas)
SELECT dev_eui, f_cnt, received_at, COUNT(*)
FROM uplinks
GROUP BY dev_eui, f_cnt, received_at
HAVING COUNT(*) > 1;
