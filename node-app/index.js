import fetch from 'node-fetch';
import { Client } from 'pg';
import dotenv from 'dotenv';
import { DateTime } from 'luxon';

dotenv.config();

// Configuración de Postgres
const client = new Client({
  user: process.env.POSTGRES_USER,
  host: 'postgres',
  database: process.env.POSTGRES_DB,
  password: process.env.POSTGRES_PASSWORD,
  port: 5432
});

await client.connect();
console.log("Conectado a PostgreSQL");

// Crear tablas si no existen
await client.query(`
CREATE TABLE IF NOT EXISTS devices (
  dev_eui VARCHAR(50) PRIMARY KEY,
  device_id TEXT,
  application_id TEXT,
  dev_addr TEXT,
  join_eui VARCHAR(50)
);
`);

await client.query(`
CREATE TABLE IF NOT EXISTS uplinks (
  id SERIAL PRIMARY KEY,
  dev_eui VARCHAR(50) REFERENCES devices(dev_eui),
  received_at TIMESTAMPTZ,
  f_port INT,
  f_cnt INT,
  frm_payload TEXT,
  decoded_payload JSONB,
  consumed_airtime TEXT,
  network_ids JSONB,
  UNIQUE(dev_eui, f_cnt)
);
`);

await client.query(`
CREATE TABLE IF NOT EXISTS rx_metadata (
  id SERIAL PRIMARY KEY,
  uplink_id INT REFERENCES uplinks(id),
  gateway_id TEXT,
  eui TEXT,
  timestamp BIGINT,
  fine_timestamp BIGINT,
  rssi INT,
  channel_rssi INT,
  snr NUMERIC,
  frequency_offset TEXT,
  channel_index INT,
  received_at TIMESTAMPTZ,
  gps_time TIMESTAMPTZ
);
`);

await client.query(`
CREATE TABLE IF NOT EXISTS uplinks_waterlevels (
  id INT PRIMARY KEY,
  dev_eui VARCHAR(50),
  received_at TIMESTAMPTZ,
  type TEXT,
  waterlevel_cm NUMERIC
);
`);

console.log("Tablas creadas o ya existentes");

// Poblar histórico de uplinks_waterlevels
await client.query(`
INSERT INTO uplinks_waterlevels (id, dev_eui, received_at, type, waterlevel_cm)
SELECT 
    id,
    dev_eui,
    received_at,
    decoded_payload->>'type',
    (regexp_replace(decoded_payload->>'waterLevel', '[^0-9\\.]', '', 'g')::numeric / 10)
FROM uplinks
WHERE decoded_payload @> '{"type": "WaterLevelDetection"}'
ON CONFLICT (id) DO NOTHING;
`);
console.log("uplinks_waterlevels poblada con datos históricos");

// Variables de configuración
const HISTORIC_LAST = process.env.SENTINEL_LAST_HISTORIC || "160h";
const INTERVAL_MIN = parseInt(process.env.SENTINEL_INTERVAL_MIN || "60");
const INTERVAL_LAST = process.env.SENTINEL_LAST_INTERVAL || "6h";

// Función para determinar si es primera ejecución
async function isFirstRun() {
  const res = await client.query("SELECT COUNT(*) as count FROM uplinks");
  return parseInt(res.rows[0].count) === 0;
}

// Función para consumir API de Sentinel y guardar en PostgreSQL
async function fetchAndSave() {
  const lastParam = (await isFirstRun()) ? HISTORIC_LAST : INTERVAL_LAST;
  let url = `https://sentinel.nam1.cloud.thethings.industries/api/v3/as/applications/${process.env.SENTINEL_APP}/packages/storage/uplink_message?last=${lastParam}`;
  let totalRecords = 0;

  try {
    while (url) {
      const res = await fetch(url, {
        headers: {
          "Authorization": `Bearer ${process.env.SENTINEL_API_TOKEN}`,
          "Accept": "application/json"
        }
      });

      const text = await res.text();
      const lines = text.split("\n").filter(Boolean);

      for (const line of lines) {
        let parsedLine;
        try {
          parsedLine = JSON.parse(line);
        } catch {
          console.warn("Registro inválido JSON ignorado:", line);
          continue;
        }

        if (!parsedLine.result || !parsedLine.result.end_device_ids) {
          console.warn("Registro sin end_device_ids ignorado:", parsedLine);
          continue;
        }

        const obj = parsedLine.result;

        // Insert device
        await client.query(`
          INSERT INTO devices(dev_eui, device_id, application_id, dev_addr, join_eui)
          VALUES($1,$2,$3,$4,$5)
          ON CONFLICT (dev_eui) DO NOTHING
        `, [
          obj.end_device_ids.dev_eui,
          obj.end_device_ids.device_id,
          obj.end_device_ids.application_ids.application_id,
          obj.end_device_ids.dev_addr,
          obj.end_device_ids.join_eui
        ]);

        // Insert/update uplink
        const uplinkRes = await client.query(`
          INSERT INTO uplinks(dev_eui, received_at, f_port, f_cnt, frm_payload, decoded_payload, consumed_airtime, network_ids)
          VALUES($1,$2,$3,$4,$5,$6,$7,$8)
          ON CONFLICT (dev_eui, f_cnt) DO UPDATE SET
            received_at = EXCLUDED.received_at,
            frm_payload = EXCLUDED.frm_payload,
            decoded_payload = EXCLUDED.decoded_payload,
            consumed_airtime = EXCLUDED.consumed_airtime,
            network_ids = EXCLUDED.network_ids
          RETURNING id
        `, [
          obj.end_device_ids.dev_eui,
          obj.received_at,
          obj.uplink_message.f_port,
          obj.uplink_message.f_cnt,
          obj.uplink_message.frm_payload,
          obj.uplink_message.decoded_payload,
          obj.uplink_message.consumed_airtime,
          obj.uplink_message.network_ids
        ]);

        const uplinkId = uplinkRes.rows[0].id;

        // Actualizar uplinks_waterlevels si es WaterLevelDetection
        const decoded = obj.uplink_message.decoded_payload;
        if (decoded && decoded.type === "WaterLevelDetection" && decoded.waterLevel) {
          const cleanValue = parseFloat((decoded.waterLevel || "").replace(/[^0-9.]/g, ""));
          const waterlevelCm = cleanValue / 10;

          await client.query(`
            INSERT INTO uplinks_waterlevels(id, dev_eui, received_at, type, waterlevel_cm)
            VALUES($1,$2,$3,$4,$5)
            ON CONFLICT (id) DO UPDATE SET
              dev_eui = EXCLUDED.dev_eui,
              received_at = EXCLUDED.received_at,
              type = EXCLUDED.type,
              waterlevel_cm = EXCLUDED.waterlevel_cm
          `, [
            uplinkId,
            obj.end_device_ids.dev_eui,
            obj.received_at,
            decoded.type,
            waterlevelCm
          ]);
        }

        // Insertar rx_metadata
        if (obj.uplink_message.rx_metadata) {
          await client.query("DELETE FROM rx_metadata WHERE uplink_id = $1", [uplinkId]);

          for (const meta of obj.uplink_message.rx_metadata) {
            await client.query(`
              INSERT INTO rx_metadata(uplink_id, gateway_id, eui, timestamp, fine_timestamp, rssi, channel_rssi, snr, frequency_offset, channel_index, received_at, gps_time)
              VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            `, [
              uplinkId,
              meta.gateway_ids.gateway_id,
              meta.gateway_ids.eui || null,
              meta.timestamp || null,
              meta.fine_timestamp || null,
              meta.rssi || null,
              meta.channel_rssi || null,
              meta.snr || null,
              meta.frequency_offset || null,
              meta.channel_index || null,
              meta.received_at || null,
              meta.gps_time || null
            ]);
          }
        }

        totalRecords++;
      }

      // Paginación
      const linkHeader = res.headers.get("Link");
      if (linkHeader && linkHeader.includes('rel="next"')) {
        const match = linkHeader.match(/<([^>]+)>;\s*rel="next"/);
        url = match ? match[1] : null;
      } else {
        url = null;
      }
    }

    console.log(`Datos de Sentinel guardados (${totalRecords} registros)`);
  } catch (err) {
    console.error("Error al traer o guardar datos:", err);
  }
}

// Ejecutar la primera vez y luego según intervalo
await fetchAndSave();
setInterval(fetchAndSave, INTERVAL_MIN * 60 * 1000);
