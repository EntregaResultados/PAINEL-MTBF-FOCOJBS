"""
Exploração: MTBF por Placa (Top 30)
Usa Databricks SQL Statement API para validar dados antes de criar o visual.
"""
import subprocess
import json

SQL = """
WITH dist AS (
    SELECT v.LicensePlate,
           MAX(s.MileageNumber) - MIN(s.MileageNumber) AS Dist_KM
    FROM hive_metastore.gold.fact_maintenanceservices s
    INNER JOIN hive_metastore.gold.dim_maintenancevehicles v
        ON s.Sk_MaintenanceVehicle = v.Sk_MaintenanceVehicle
    WHERE s.ServiceStartTimestamp >= DATE_ADD(CURRENT_DATE(), -365)
      AND s.MileageNumber BETWEEN 100 AND 900000
      AND v.AdditionalInformation3Description IN 
          ('REGIONAL 1','REGIONAL 2','REGIONAL 3','REGIONAL 4',
           'REGIONAL 5','REGIONAL 6','REGIONAL 7','REGIONAL 8')
    GROUP BY v.LicensePlate
    HAVING MAX(s.MileageNumber) - MIN(s.MileageNumber) > 0
),
falhas AS (
    SELECT v.LicensePlate,
           COUNT(DISTINCT CONCAT(v.LicensePlate, '|', CAST(DATE(s.ServiceStartTimestamp) AS STRING))) AS Qtd
    FROM hive_metastore.gold.fact_maintenanceservices s
    INNER JOIN hive_metastore.gold.dim_maintenancevehicles v
        ON s.Sk_MaintenanceVehicle = v.Sk_MaintenanceVehicle
    INNER JOIN hive_metastore.gold.dim_maintenancetypes dt
        ON s.Sk_MaintenanceType = dt.Sk_MaintenanceType
    INNER JOIN hive_metastore.gold.dim_maintenanceserviceorderstatustypes ds
        ON s.Sk_ServiceOrderStatusType = ds.Sk_ServiceOrderStatusType
    INNER JOIN hive_metastore.gold.fact_maintenanceitems fi
        ON s.Sk_MaintenanceServices = fi.Sk_MaintenanceServices
    LEFT JOIN hive_metastore.gold.dim_maintenanceparts dp
        ON fi.Sk_MaintenancePart = dp.Sk_MaintenancePart
    WHERE s.ServiceStartTimestamp >= DATE_ADD(CURRENT_DATE(), -365)
      AND s.MileageNumber BETWEEN 100 AND 900000
      AND v.AdditionalInformation3Description IN 
          ('REGIONAL 1','REGIONAL 2','REGIONAL 3','REGIONAL 4',
           'REGIONAL 5','REGIONAL 6','REGIONAL 7','REGIONAL 8')
      AND dt.MaintenanceType = 'Corretiva'
      AND ds.StatusTypeDescription IN ('Cobradas','Concluidas E Nao Cobradas','Aprovadas','Aprovadas Parcialmente')
      AND COALESCE(dp.PartGroupName, '') NOT IN ('Funilaria','Acessorios')
    GROUP BY v.LicensePlate
)
SELECT d.LicensePlate AS Placa,
       d.Dist_KM,
       f.Qtd AS Falhas,
       ROUND(d.Dist_KM / f.Qtd, 0) AS MTBF_KM
FROM dist d
INNER JOIN falhas f ON d.LicensePlate = f.LicensePlate
ORDER BY MTBF_KM DESC
LIMIT 30
"""

payload = json.dumps({
    "warehouse_id": "ce56ec5f5d0a3e07",
    "statement": SQL.strip(),
    "wait_timeout": "30s"
})

result = subprocess.run(
    ["databricks", "api", "post", "/api/2.0/sql/statements", "--json", payload],
    capture_output=True, text=True
)

if result.returncode != 0:
    print("ERRO:", result.stderr)
else:
    data = json.loads(result.stdout)
    status = data.get("status", {}).get("state", "UNKNOWN")
    print(f"Status: {status}")
    
    if status == "SUCCEEDED":
        manifest = data.get("manifest", {})
        cols = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
        rows = data.get("result", {}).get("data_array", [])
        
        print(f"\nColunas: {cols}")
        print(f"Total de linhas: {len(rows)}\n")
        print(f"{'Placa':<12} {'Dist_KM':>10} {'Falhas':>8} {'MTBF_KM':>10}")
        print("-" * 44)
        for row in rows:
            print(f"{row[0]:<12} {row[1]:>10} {row[2]:>8} {row[3]:>10}")
    else:
        print("Resposta completa:")
        print(json.dumps(data, indent=2)[:3000])
