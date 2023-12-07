/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package benchmark

class ETLQueries(
        dbLocation: String,
        formatName: String,
        sourceLocation: String,
        sourceFormat: String,
        tblProperties: String) {

  val prepQueries: Map[String, String] = Map(
    // Create the initial denormalized store_sales with all the rows
    "etlPrep1-createDenormTable-AllData" ->
      s"""
      CREATE TABLE IF NOT EXISTS store_sales_denorm
      USING ${formatName}
      OPTIONS ('compression'='snappy')
      LOCATION '${dbLocation}/store_sales_denorm'
      PARTITIONED BY (ss_sold_date_sk) AS
      SELECT *
        FROM `${sourceFormat}`.`${sourceLocation}store_sales`,
             `${sourceFormat}`.`${sourceLocation}date_dim`,
             `${sourceFormat}`.`${sourceLocation}time_dim`,
             `${sourceFormat}`.`${sourceLocation}customer`,
             `${sourceFormat}`.`${sourceLocation}customer_demographics`,
             `${sourceFormat}`.`${sourceLocation}household_demographics`,
             `${sourceFormat}`.`${sourceLocation}customer_address`,
             `${sourceFormat}`.`${sourceLocation}store`,
             `${sourceFormat}`.`${sourceLocation}promotion`,
             `${sourceFormat}`.`${sourceLocation}item`
      WHERE ss_sold_date_sk = d_date_sk
        AND ss_store_sk     = s_store_sk
        AND ss_sold_time_sk = t_time_sk
        AND ss_item_sk      = i_item_sk
        AND ss_customer_sk  = c_customer_sk
        AND ss_cdemo_sk     = cd_demo_sk
        AND ss_hdemo_sk     = hd_demo_sk
        AND ss_addr_sk      = ca_address_sk
        AND ss_promo_sk     = p_promo_sk
      DISTRIBUTE BY ss_sold_date_sk
      """,
    // Create the source table using the full data, excluding ~1.5% for later insert
    "etlPrep2-createDenormTable-StartData" ->
      s"""
      CREATE TABLE IF NOT EXISTS store_sales_denorm_start
       USING ${formatName}
      OPTIONS ('compression'='snappy')
      LOCATION '${dbLocation}/store_sales_denorm_start'
      PARTITIONED BY (ss_sold_date_sk) AS
      SELECT *
        FROM store_sales_denorm
       WHERE MOD(ss_sold_date_sk, 2) <> 0
          OR ss_sold_date_sk <= 2452459
          OR MOD(ss_sold_time_sk, 5) <> 0
      DISTRIBUTE BY ss_sold_date_sk
      """,
    // Create the source table w/ the upsert data, the ~1.5% excluded from `_start`
    "etlPrep3-createDenormTable-UpsertMedium" ->
      s"""
      CREATE TABLE IF NOT EXISTS store_sales_denorm_upsert
      USING ${formatName}
      OPTIONS ('compression'='snappy')
      LOCATION '${dbLocation}/store_sales_denorm_upsert'
      PARTITIONED BY (ss_sold_date_sk) AS
      SELECT *
        FROM store_sales_denorm
       WHERE MOD(ss_sold_date_sk, 2) = 0
         AND MOD(ss_sold_time_sk, 5) = 0 
         AND ss_sold_date_sk > 2452459
      DISTRIBUTE BY ss_sold_date_sk
      """,
    // Table for the insert (not currently used during write)
    "etlPrep4-createDenormTable-InsertMedium" ->
      s"""
      CREATE TABLE IF NOT EXISTS store_sales_denorm_insert_medium
      USING ${formatName}
      OPTIONS ('compression'='snappy')
      LOCATION '${dbLocation}/store_sales_denorm_insert_medium'
      PARTITIONED BY (ss_sold_date_sk) AS
      SELECT *
        FROM store_sales_denorm
       WHERE MOD(ss_sold_date_sk, 2) = 1
         AND MOD(ss_sold_time_sk, 25) = 0
         AND ss_sold_date_sk > 2452459
      DISTRIBUTE BY ss_sold_date_sk
      """,
    // Create the X-Small delete data selection (0.015%)
    "etlPrep5-createDenormTable-DeleteXSmall" ->
      s"""
      CREATE TABLE IF NOT EXISTS store_sales_denorm_delete_xsmall
       USING ${formatName}
      OPTIONS ('compression'='snappy')
      LOCATION '${dbLocation}/store_sales_denorm_delete_xsmall'
      PARTITIONED BY (ss_sold_date_sk) AS
      SELECT *
        FROM store_sales_denorm
       WHERE MOD(ss_sold_date_sk, 10) = 1
         AND MOD(ss_sold_time_sk, 100) = 0
         AND ss_sold_date_sk > 2452459
      DISTRIBUTE BY ss_sold_date_sk
      """,
    // Create the Small delete data selection (0.15%)
    "etlPrep6-createDenormTable-DeleteSmall" ->
      s"""
      CREATE TABLE IF NOT EXISTS store_sales_denorm_delete_small
       USING ${formatName}
      OPTIONS ('compression'='snappy')
      LOCATION '${dbLocation}/store_sales_denorm_delete_small'
      PARTITIONED BY (ss_sold_date_sk) AS
      SELECT *
        FROM store_sales_denorm
       WHERE MOD(ss_sold_date_sk, 10) = 0
         AND MOD(ss_sold_time_sk, 10) = 0
         AND ss_sold_date_sk > 2452459
      DISTRIBUTE BY ss_sold_date_sk
      """,
    // Create the Medium delete data selection (1.5%)
    "etlPrep7-createDenormTable-DeleteMedium" ->
      s"""
      CREATE TABLE IF NOT EXISTS store_sales_denorm_delete_medium
       USING ${formatName}
      OPTIONS ('compression'='snappy')
      LOCATION '${dbLocation}/store_sales_denorm_delete_medium'
      PARTITIONED BY (ss_sold_date_sk) AS
      SELECT *
        FROM store_sales_denorm
       WHERE MOD(ss_sold_date_sk, 3) = 0
         AND MOD(ss_sold_time_sk, 3) = 0
         AND ss_sold_date_sk > 2452459
      DISTRIBUTE BY ss_sold_date_sk
      """
  )

  val writeQueries: Map[String, String] = Map(
    // Step 1 - Bulk load the starting point table
    "etl1-createTable" ->
      s"""
      CREATE TABLE store_sales_denorm_${formatName}
      USING ${formatName}
      LOCATION '${dbLocation}/store_sales_denorm'
      PARTITIONED BY (ss_sold_date_sk)
      ${tblProperties}
      AS SELECT * FROM `${sourceFormat}`.`${sourceLocation}store_sales_denorm_start`
      """,
    // Step 2 - Add the Medium Upsert data into the table
    "etl2-upsertMedium" ->
      s"""
      MERGE INTO store_sales_denorm_${formatName} AS a
      USING `${sourceFormat}`.`${sourceLocation}store_sales_denorm_upsert` AS b
      ON a.ss_sold_date_sk = b.ss_sold_date_sk
      AND a.ss_item_sk = b.ss_item_sk
      AND a.ss_ticket_number = b.ss_ticket_number
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
      """,
    // Step 3 - Remove the X-Small Delete data from the table
    "etl3-deleteXSmall" ->
      s"""
      MERGE INTO store_sales_denorm_${formatName} AS a
      USING `${sourceFormat}`.`${sourceLocation}store_sales_denorm_delete_xsmall` AS b
      ON a.ss_sold_date_sk = b.ss_sold_date_sk
      AND a.ss_item_sk = b.ss_item_sk
      AND a. ss_ticket_number = b. ss_ticket_number
      WHEN MATCHED THEN DELETE
      """,
    // Step 4 - Remove the Small Delete data from the table
    "etl4-deleteSmall" ->
      s"""
      MERGE INTO store_sales_denorm_${formatName} AS a
      USING `${sourceFormat}`.`${sourceLocation}store_sales_denorm_delete_small` AS b
      ON a.ss_sold_date_sk = b.ss_sold_date_sk
      AND a.ss_item_sk = b.ss_item_sk
      AND a. ss_ticket_number = b. ss_ticket_number
      WHEN MATCHED THEN DELETE
      """,
    // Step 5 - Remove the Medium Delete data from the table
    "etl5-deleteMedium" ->
      s"""
      MERGE INTO store_sales_denorm_${formatName} AS a
      USING `${sourceFormat}`.`${sourceLocation}store_sales_denorm_delete_medium` AS b
      ON a.ss_sold_date_sk = b.ss_sold_date_sk
      AND a.ss_item_sk = b.ss_item_sk
      AND a. ss_ticket_number = b. ss_ticket_number
      WHEN MATCHED THEN DELETE
      """,
    // Step 6 - Delete a Single Customer (GDPR request) from the table
    "etl6-deleteGdpr" ->
      s"""
      DELETE FROM store_sales_denorm_${formatName} WHERE c_customer_sk = 221580
      """
    )

  val readQueries: Map[String, String] = Map(
   "q3"  ->
     s"""
     SELECT d_year
          , i_brand_id              brand_id
          , i_brand                 brand
          , SUM(ss_ext_sales_price) sum_agg
       FROM store_sales_denorm_${formatName}
      WHERE i_manufact_id = 436
        AND d_moy=12
      GROUP BY d_year, i_brand, i_brand_id
      ORDER BY d_year, sum_agg DESC, brand_id
      LIMIT 100
     """,
   "q6"  ->
     s"""
     SELECT s.ca_state state
          , COUNT(*) cnt
       FROM store_sales_denorm_${formatName} s
      WHERE s.d_month_seq = (SELECT DISTINCT (d_month_seq)
                             FROM store_sales_denorm_${formatName}
                             WHERE d_year = 2000 AND d_moy = 2 )
        AND s.i_current_price > 1.2 * (SELECT avg(j.i_current_price)
                                       FROM store_sales_denorm_${formatName} j
                                       WHERE j.i_category = s.i_category )
      GROUP BY s.ca_state
     HAVING count(*) >= 10
      ORDER BY cnt, s.ca_state
      LIMIT 100
     """,
   "q7"  ->
     s"""
     SELECT i_item_id
          , AVG(ss_quantity)    agg1
          , AVG(ss_list_price)  agg2
          , AVG(ss_coupon_amt)  agg3
          , AVG(ss_sales_price) agg4
     FROM store_sales_denorm_${formatName}
      WHERE cd_gender = 'F'
        AND cd_marital_status = 'W'
        AND cd_education_status = 'Primary'
        AND (p_channel_email = 'N'
         OR p_channel_event = 'N')
        AND d_year = 1998
      GROUP BY i_item_id
      ORDER BY i_item_id
      LIMIT 100
     """,
   "q8"  ->
     s"""
     SELECT s_store_name
          , SUM(ss_net_profit)
       FROM store_sales_denorm_${formatName}
          , (SELECT ca_zip
               FROM (SELECT SUBSTR(ca_zip, 1, 5) ca_zip
                       FROM store_sales_denorm_${formatName}
                      WHERE substr(ca_zip,1,5) IN ( '89436', '30868', '65085', '22977', '83927', '77557', '58429', '40697'
                                                  , '80614', '10502', '32779', '91137', '61265', '98294', '17921', '18427'
                                                  , '21203', '59362', '87291', '84093', '21505', '17184', '10866', '67898'
                                                  , '25797', '28055', '18377', '80332', '74535', '21757', '29742', '90885'
                                                  , '29898', '17819', '40811', '25990', '47513', '89531', '91068', '10391'
                                                  , '18846', '99223', '82637', '41368', '83658', '86199', '81625', '26696'
                                                  , '89338', '88425', '32200', '81427', '19053', '77471', '36610', '99823'
                                                  , '43276', '41249', '48584', '83550', '82276', '18842', '78890', '14090'
                                                  , '38123', '40936', '34425', '19850', '43286', '80072', '79188', '54191'
                                                  , '11395', '50497', '84861', '90733', '21068', '57666', '37119', '25004'
                                                  , '57835', '70067', '62878', '95806', '19303', '18840', '19124', '29785'
                                                  , '16737', '16022', '49613', '89977', '68310', '60069', '98360', '48649'
                                                  , '39050', '41793', '25002', '27413', '39736', '47208', '16515', '94808'
                                                  , '57648', '15009', '80015', '42961', '63982', '21744', '71853', '81087'
                                                  , '67468', '34175', '64008', '20261', '11201', '51799', '48043', '45645'
                                                  , '61163', '48375', '36447', '57042', '21218', '41100', '89951', '22745'
                                                  , '35851', '83326', '61125', '78298', '80752', '49858', '52940', '96976'
                                                  , '63792', '11376', '53582', '18717', '90226', '50530', '94203', '99447'
                                                  , '27670', '96577', '57856', '56372', '16165', '23427', '54561', '28806'
                                                  , '44439', '22926', '30123', '61451', '92397', '56979', '92309', '70873'
                                                  , '13355', '21801', '46346', '37562', '56458', '28286', '47306', '99555'
                                                  , '69399', '26234', '47546', '49661', '88601', '35943', '39936', '25632'
                                                  , '24611', '44166', '56648', '30379', '59785', '11110', '14329', '93815'
                                                  , '52226', '71381', '13842', '25612', '63294', '14664', '21077', '82626'
                                                  , '18799', '60915', '81020', '56447', '76619', '11433', '13414', '42548'
                                                  , '92713', '70467', '30884', '47484', '16072', '38936', '13036', '88376'
                                                  , '45539', '35901', '19506', '65690', '73957', '71850', '49231', '14276'
                                                  , '20005', '18384', '76615', '11635', '38177', '55607', '41369', '95447'
                                                  , '58581', '58149', '91946', '33790', '76232', '75692', '95464', '22246'
                                                  , '51061', '56692', '53121', '77209', '15482', '10688', '14868', '45907'
                                                  , '73520', '72666', '25734', '17959', '24677', '66446', '94627', '53535'
                                                  , '15560', '41967', '69297', '11929', '59403', '33283', '52232', '57350'
                                                  , '43933', '40921', '36635', '10827', '71286', '19736', '80619', '25251'
                                                  , '95042', '15526', '36496', '55854', '49124', '81980', '35375', '49157'
                                                  , '63512', '28944', '14946', '36503', '54010', '18767', '23969', '43905'
                                                  , '66979', '33113', '21286', '58471', '59080', '13395', '79144', '70373'
                                                  , '67031', '38360', '26705', '50906', '52406', '26066', '73146', '15884'
                                                  , '31897', '30045', '61068', '45550', '92454', '13376', '14354', '19770'
                                                  , '22928', '97790', '50723', '46081', '30202', '14410', '20223', '88500'
                                                  , '67298', '13261', '14172', '81410', '93578', '83583', '46047', '94167'
                                                  , '82564', '21156', '15799', '86709', '37931', '74703', '83103', '23054'
                                                  , '70470', '72008', '49247', '91911', '69998', '20961', '70070', '63197'
                                                  , '54853', '88191', '91830', '49521', '19454', '81450', '89091', '62378'
                                                  , '25683', '61869', '51744', '36580', '85778', '36871', '48121', '28810'
                                                  , '83712', '45486', '67393', '26935', '42393', '20132', '55349', '86057'
                                                  , '21309', '80218', '10094', '11357', '48819', '39734', '40758', '30432'
                                                  , '21204', '29467', '30214', '61024', '55307', '74621', '11622', '68908'
                                                  , '33032', '52868', '99194', '99900', '84936', '69036', '99149', '45013'
                                                  , '32895', '59004', '32322', '14933', '32936', '33562', '72550', '27385'
                                                  , '58049', '58200', '16808', '21360', '32961', '18586', '79307', '15492')
                     INTERSECT
                     SELECT ca_zip
                       FROM (SELECT substr(ca_zip,1,5) ca_zip
                                  , count(*) cnt
                               FROM store_sales_denorm_${formatName}
                              WHERE c_preferred_cust_flag='Y'
                              GROUP BY ca_zip
                             HAVING count(*)> 10) A1
                    ) A2
           ) V1
      WHERE d_qoy = 1 AND d_year = 2002
        AND (substr(s_zip, 1, 2) = substr(V1.ca_zip, 1, 2))
      GROUP BY s_store_name
      ORDER BY s_store_name
      LIMIT 100
     """,
   "q9"  ->
     s"""
     SELECT CASE WHEN (SELECT COUNT(*)
                       FROM store_sales_denorm_${formatName}
                       WHERE ss_quantity BETWEEN 1 AND 20 ) > 48409437
                THEN (SELECT AVG(ss_ext_discount_amt)
                      FROM store_sales_denorm_${formatName}
                      WHERE ss_quantity BETWEEN 1 AND 20 )
                ELSE (SELECT AVG(ss_net_profit)
                      FROM store_sales_denorm_${formatName}
                      WHERE ss_quantity BETWEEN 1 AND 20 )
            END bucket1
          , CASE WHEN (SELECT COUNT(*)
                       FROM store_sales_denorm_${formatName}
                       WHERE ss_quantity BETWEEN 21 AND 40 ) > 24804257
                THEN (SELECT AVG(ss_ext_discount_amt)
                      FROM store_sales_denorm_${formatName}
                      WHERE ss_quantity BETWEEN 21 AND 40 )
                ELSE (SELECT AVG(ss_net_profit)
                      FROM store_sales_denorm_${formatName}
                      WHERE ss_quantity BETWEEN 21 AND 40 )
            END bucket2
          , CASE WHEN (SELECT COUNT(*)
                       FROM store_sales_denorm_${formatName}
                       WHERE ss_quantity BETWEEN 41 AND 60 ) > 128048939
                THEN (SELECT AVG(ss_ext_discount_amt)
                      FROM store_sales_denorm_${formatName}
                      WHERE ss_quantity BETWEEN 41 AND 60 )
                ELSE (SELECT AVG(ss_net_profit)
                      FROM store_sales_denorm_${formatName}
                      WHERE ss_quantity BETWEEN 41 AND 60 )
            END bucket3
          , CASE WHEN (SELECT COUNT(*)
                       FROM store_sales_denorm_${formatName}
                       WHERE ss_quantity BETWEEN 61 AND 80 ) > 56503968
                THEN (SELECT AVG(ss_ext_discount_amt)
                      FROM store_sales_denorm_${formatName}
                      WHERE ss_quantity BETWEEN 61 AND 80 )
                ELSE (SELECT AVG(ss_net_profit)
                      FROM store_sales_denorm_${formatName}
                      WHERE ss_quantity BETWEEN 61 AND 80 )
            END bucket4
          , CASE WHEN (SELECT COUNT(*)
                       FROM store_sales_denorm_${formatName}
                       WHERE ss_quantity BETWEEN 81 AND 100 ) > 43571537
                THEN (SELECT AVG(ss_ext_discount_amt)
                      FROM store_sales_denorm_${formatName}
                      WHERE ss_quantity BETWEEN 81 AND 100 )
                ELSE (SELECT AVG(ss_net_profit)
                      FROM store_sales_denorm_${formatName}
                      WHERE ss_quantity BETWEEN 81 AND 100 )
            END bucket5
     """,
   "q13" ->
     s"""
     SELECT AVG(ss_quantity)
          , AVG(ss_ext_sales_price)
          , AVG(ss_ext_wholesale_cost)
          , SUM(ss_ext_wholesale_cost)
     FROM store_sales_denorm_${formatName}
     WHERE d_year = 2001
       AND ((cd_marital_status = 'D'
       AND cd_education_status = '2 yr Degree'
       AND ss_sales_price BETWEEN 100.00 AND 150.00
       AND hd_dep_count = 3 )
        OR (cd_marital_status = 'S'
       AND cd_education_status = 'Secondary'
       AND ss_sales_price BETWEEN 50.00 AND 100.00
       AND hd_dep_count = 1 )
        OR (cd_marital_status = 'W'
       AND cd_education_status = 'Advanced Degree'
       AND ss_sales_price BETWEEN 150.00 AND 200.00
       AND hd_dep_count = 1 ))
       AND ((ca_country = 'United States'
       AND ca_state IN ('CO', 'IL', 'MN')
       AND ss_net_profit BETWEEN 100 AND 200 )
        OR (ca_country = 'United States'
       AND ca_state IN ('OH', 'MT', 'NM')
       AND ss_net_profit BETWEEN 150 AND 300 )
        OR (ca_country = 'United States'
       AND ca_state IN ('TX', 'MO', 'MI')
       AND ss_net_profit BETWEEN 50 AND 250 ))
     """,
   "q19" ->
     s"""
     SELECT i_brand_id              brand_id
          , i_brand                 brand
          , i_manufact_id
          , i_manufact
          , SUM(ss_ext_sales_price) ext_price
       FROM store_sales_denorm_${formatName}
      WHERE i_manager_id=7 AND d_moy=11 AND d_year=1999 AND substr(ca_zip, 1, 5) <> substr(s_zip, 1, 5)
      GROUP BY i_brand, i_brand_id, i_manufact_id, i_manufact
      ORDER BY ext_price DESC, i_brand, i_brand_id, i_manufact_id, i_manufact
      LIMIT 100
     """,
   "q27" ->
     s"""
     SELECT i_item_id
          , s_state
          , GROUPING (s_state) g_state
          , avg(ss_quantity) agg1
          , avg(ss_list_price) agg2
          , avg(ss_coupon_amt) agg3
          , avg(ss_sales_price) agg4
       FROM store_sales_denorm_${formatName}
      WHERE cd_gender = 'F'
        AND cd_marital_status = 'M'
        AND cd_education_status = '4 yr Degree'
        AND d_year = 2002
        AND s_state IN ('NE', 'IN', 'SD', 'MN', 'TX', 'MN')
      GROUP BY ROLLUP (i_item_id, s_state)
      ORDER BY i_item_id, s_state
      LIMIT 100
     """,
   "q28" ->
     s"""
     SELECT *
     FROM (
         SELECT AVG(ss_list_price)            B1_LP
              , COUNT(ss_list_price)          B1_CNT
              , COUNT(DISTINCT ss_list_price) B1_CNTD
           FROM store_sales_denorm_${formatName}
          WHERE ss_quantity BETWEEN 0 AND 5
            AND (ss_list_price BETWEEN 11 AND 11+10
             OR ss_coupon_amt BETWEEN 460 AND 460+1000
             OR ss_wholesale_cost BETWEEN 14 AND 14+20)
          ) B1
        , (
         SELECT AVG(ss_list_price)            B2_LP
              , COUNT(ss_list_price)          B2_CNT
              , COUNT(DISTINCT ss_list_price) B2_CNTD
           FROM store_sales_denorm_${formatName}
          WHERE ss_quantity BETWEEN 6 AND 10
            AND (ss_list_price BETWEEN 91 AND 91+10
             OR ss_coupon_amt BETWEEN 1430 AND 1430+1000
             OR ss_wholesale_cost BETWEEN 32 AND 32+20)
          ) B2
        , (
         SELECT AVG(ss_list_price)            B3_LP
              , COUNT(ss_list_price)          B3_CNT
              , COUNT(DISTINCT ss_list_price) B3_CNTD
           FROM store_sales_denorm_${formatName}
          WHERE ss_quantity BETWEEN 11 AND 15
            AND (ss_list_price BETWEEN 66 AND 66+10
             OR ss_coupon_amt BETWEEN 920 AND 920+1000
             OR ss_wholesale_cost BETWEEN 4 AND 4+20)
          ) B3
        , (
         SELECT AVG(ss_list_price)            B4_LP
              , COUNT(ss_list_price)          B4_CNT
              , COUNT(DISTINCT ss_list_price) B4_CNTD
           FROM store_sales_denorm_${formatName}
          WHERE ss_quantity BETWEEN 16 AND 20
            AND (ss_list_price BETWEEN 142 AND 142+10
             OR ss_coupon_amt BETWEEN 3054 AND 3054+1000
             OR ss_wholesale_cost BETWEEN 80 AND 80+20)
          ) B4
        , (
         SELECT AVG(ss_list_price)            B5_LP
              , COUNT(ss_list_price)          B5_CNT
              , COUNT(DISTINCT ss_list_price) B5_CNTD
           FROM store_sales_denorm_${formatName}
          WHERE ss_quantity BETWEEN 21 AND 25
            AND (ss_list_price BETWEEN 135 AND 135+10
             OR ss_coupon_amt BETWEEN 14180 AND 14180+1000
             OR ss_wholesale_cost BETWEEN 38 AND 38+20)
          ) B5
        , (
         SELECT AVG(ss_list_price)            B6_LP
              , COUNT(ss_list_price)          B6_CNT
              , COUNT(DISTINCT ss_list_price) B6_CNTD
           FROM store_sales_denorm_${formatName}
          WHERE ss_quantity BETWEEN 26 AND 30
            AND (ss_list_price BETWEEN 28 AND 28+10
             OR ss_coupon_amt BETWEEN 2513 AND 2513+1000
             OR ss_wholesale_cost BETWEEN 42 AND 42+20)
          ) B6
     LIMIT 100
     """,
   "q36" ->
     s"""
     SELECT SUM(ss_net_profit) / SUM(ss_ext_sales_price) AS gross_margin
          , i_category
          , i_class
          , GROUPING (i_category)+ GROUPING (i_class) AS lochierarchy
          , rank() OVER ( PARTITION BY GROUPING (i_category)+ GROUPING (i_class)
          , CASE WHEN GROUPING (i_class) = 0 THEN i_category END ORDER BY sum(ss_net_profit)/sum(ss_ext_sales_price) ASC) AS rank_within_parent
       FROM store_sales_denorm_${formatName}
      WHERE d_year = 1999 AND s_state IN ('NE', 'IN', 'SD', 'MN', 'TX', 'MN', 'MI', 'LA')
      GROUP BY ROLLUP (i_category, i_class)
      ORDER BY lochierarchy DESC, CASE WHEN lochierarchy = 0 THEN i_category END, rank_within_parent
      LIMIT 100
     """
   )
}
