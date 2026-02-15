-- Exploração: MTBF por Placa (Top 30)
-- Calcula MTBF = (MAX(KM) - MIN(KM)) / COUNT(DISTINCT Placa+Data eventos corretivos)
-- Filtros: KM 100-900000, Corretiva, Status válido, REGIONAL JBS, último ano
-- Exclui Funilaria e Acessorios

WITH services_filtrado AS (
    SELECT 
        fs.Sk_MaintenanceServices,
        fs.Sk_MaintenanceVehicle,
        fs.MileageNumber,
        fs.ServiceStartTimestamp
    FROM hive_metastore.gold.fact_maintenanceservices fs
    WHERE fs.ServiceStartTimestamp >= DATE_ADD(CURRENT_DATE(), -365)
      AND fs.MileageNumber >= 100
      AND fs.MileageNumber <= 900000
),
veiculos AS (
    SELECT 
        dv.Sk_MaintenanceVehicle,
        dv.LicensePlate,
        dv.AdditionalInformation3Description AS Regional
    FROM hive_metastore.gold.dim_maintenancevehicles dv
    WHERE dv.AdditionalInformation3Description IN (
        'REGIONAL 1','REGIONAL 2','REGIONAL 3','REGIONAL 4',
        'REGIONAL 5','REGIONAL 6','REGIONAL 7','REGIONAL 8'
    )
),
joined AS (
    SELECT 
        sf.Sk_MaintenanceServices,
        v.LicensePlate,
        sf.MileageNumber,
        sf.ServiceStartTimestamp,
        v.Regional
    FROM services_filtrado sf
    INNER JOIN veiculos v ON sf.Sk_MaintenanceVehicle = v.Sk_MaintenanceVehicle
),
-- Join com items para pegar tipo manutencao e grupo pecas
items_join AS (
    SELECT 
        j.*,
        dt.MaintenanceType,
        ds.StatusTypeDescription,
        dp.PartGroupName
    FROM joined j
    INNER JOIN hive_metastore.gold.fact_maintenanceitems fi 
        ON j.Sk_MaintenanceServices = fi.Sk_MaintenanceServices
    LEFT JOIN hive_metastore.gold.dim_maintenancetypes dt 
        ON fi.Sk_MaintenanceType = dt.Sk_MaintenanceType
    LEFT JOIN hive_metastore.gold.dim_maintenanceserviceorderstatustypes ds 
        ON fi.Sk_ServiceOrderStatusType = ds.Sk_ServiceOrderStatusType
    LEFT JOIN hive_metastore.gold.dim_maintenanceparts dp 
        ON fi.Sk_MaintenancePart = dp.Sk_MaintenancePart
),
-- Distancia por placa
distancia AS (
    SELECT 
        LicensePlate,
        MAX(MileageNumber) - MIN(MileageNumber) AS Distancia_KM
    FROM items_join
    GROUP BY LicensePlate
    HAVING MAX(MileageNumber) - MIN(MileageNumber) > 0
),
-- Falhas (eventos distintos Placa+Data) - somente corretiva, excluindo funilaria/acessorios
falhas AS (
    SELECT 
        LicensePlate,
        COUNT(DISTINCT CONCAT(LicensePlate, '|', CAST(DATE(ServiceStartTimestamp) AS STRING))) AS Qtd_Falhas
    FROM items_join
    WHERE MaintenanceType = 'Corretiva'
      AND StatusTypeDescription IN ('Cobradas', 'Concluidas E Nao Cobradas', 'Aprovadas', 'Aprovadas Parcialmente')
      AND PartGroupName NOT IN ('Funilaria', 'Acessorios')
    GROUP BY LicensePlate
)
SELECT 
    d.LicensePlate AS Placa,
    d.Distancia_KM,
    COALESCE(f.Qtd_Falhas, 0) AS Qtd_Falhas,
    CASE WHEN COALESCE(f.Qtd_Falhas, 0) > 0 
         THEN ROUND(d.Distancia_KM / f.Qtd_Falhas, 0)
         ELSE NULL END AS MTBF_KM
FROM distancia d
LEFT JOIN falhas f ON d.LicensePlate = f.LicensePlate
WHERE COALESCE(f.Qtd_Falhas, 0) > 0
ORDER BY MTBF_KM DESC
LIMIT 30
