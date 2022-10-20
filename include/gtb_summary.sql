
SELECT DATE(time_in)Trx_date, 'AIRTIME' CATEGORY, CASE WHEN real_network IS NULL THEN network ELSE real_network END Network, COUNT(DISTINCT phonenumber)Unique_count,COUNT(phonenumber) Trx_count, FORMAT(SUM(amount),2) Trx_amount
FROM gtb_airtime_request_logs         
WHERE time_in BETWEEN DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%Y-%m-%d 00:00:00') AND DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%Y-%m-%d 23:59:59')
AND completed_txn_status  NOT IN (2, 500,300)
AND operation_id NOT LIKE '%-D%'
AND txn_id NOT LIKE '%_repush%'
AND txn_id NOT LIKE '%_R1%' AND client_id = 'ZW5HJKOK-983-POQ'
GROUP BY 'AIRTIME', DATE(time_in), CASE WHEN real_network IS NULL THEN network ELSE real_network END
UNION 
SELECT DATE(time_in)Trx_date, 'DATA' CATEGORY, network, COUNT(DISTINCT phonenumber)Unique_count,COUNT(phonenumber) Trx_count,FORMAT(SUM(amount),2) Trx_amount
FROM gtb_bundle_request_logs         
WHERE time_in BETWEEN DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%Y-%m-%d 00:00:00') AND DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 1 DAY), '%Y-%m-%d 23:59:59')
AND completed_txn_status NOT  IN (2, 500,300)
AND operation_id NOT LIKE '%-D%'
AND txn_id NOT LIKE '%_repush%'
AND txn_id NOT LIKE '%_R1%' AND client_id = 'ZW5HJKOK-983-POQ'
GROUP BY 'DATA', DATE(time_in), network
