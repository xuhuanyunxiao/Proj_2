USE data_hub_new;
drop table if exists company_agri_produce;
CREATE EXTERNAL TABLE `company_agri_produce` (
	rowkey	STRING	DEFAULT NULL,
	address	STRING	DEFAULT NULL,
	area_code	STRING	DEFAULT NULL,
	certificate_resion	STRING	DEFAULT NULL,
	certificates_name	STRING	DEFAULT NULL,
	certification_type	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_info_code	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_url	STRING	DEFAULT NULL,
	credentials_code	STRING	DEFAULT NULL,
	credentials_status	STRING	DEFAULT NULL,
	permit_scope	STRING	DEFAULT NULL,
	product_name	STRING	DEFAULT NULL,
	product_type	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL,
	unvalidity_time	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_agri_produce';


USE data_hub_new;
drop table if exists company_agri_produces_info;
CREATE EXTERNAL TABLE `company_agri_produces_info` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_url	STRING	DEFAULT NULL,
	credentials_code	STRING	DEFAULT NULL,
	product_address	STRING	DEFAULT NULL,
	product_annual_output	STRING	DEFAULT NULL,
	product_class	STRING	DEFAULT NULL,
	product_material_from	STRING	DEFAULT NULL,
	product_name	STRING	DEFAULT NULL,
	product_remark	STRING	DEFAULT NULL,
	product_working_name	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_agri_produces_info';


USE data_hub_new;
drop table if exists company_aptitude;
CREATE EXTERNAL TABLE `company_aptitude` (
	rowkey	STRING	DEFAULT NULL,
	aptitude_issue_code	STRING	DEFAULT NULL,
	aptitude_period_end	STRING	DEFAULT NULL,
	aptitude_province	STRING	DEFAULT NULL,
	aptitude_regis_code	STRING	DEFAULT NULL,
	aptitude_scope	STRING	DEFAULT NULL,
	aptitude_type	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_credit_code	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_regis_code	STRING	DEFAULT NULL,
	company_url	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_aptitude';


USE data_hub_new;
drop table if exists company_balance_sheet;
CREATE EXTERNAL TABLE `company_balance_sheet` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_b_s_other_liabi	STRING	DEFAULT NULL,
	company_b_s_fixed_assets	STRING	DEFAULT NULL,
	company_b_s_intan_assets	STRING	DEFAULT NULL,
	company_b_s_inte_payable	STRING	DEFAULT NULL,
	company_b_s_long_investe	STRING	DEFAULT NULL,
	company_b_s_minor_inte	STRING	DEFAULT NULL,
	company_b_s_monet_fund	STRING	DEFAULT NULL,
	company_b_s_payr_payable	STRING	DEFAULT NULL,
	company_b_s_report_date	STRING	DEFAULT NULL,
	company_b_s_sale_fin_asse	STRING	DEFAULT NULL,
	company_b_s_share_parent	STRING	DEFAULT NULL,
	company_b_s_total_assets	STRING	DEFAULT NULL,
	company_b_s_total_equity	STRING	DEFAULT NULL,
	company_b_s_total_liabi	STRING	DEFAULT NULL,
	company_b_s_tra_fin_asse	STRING	DEFAULT NULL,
	company_b_s_undist_profit	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_balance_sheet';


USE data_hub_new;
drop table if exists company_base_business_merge_new;
CREATE EXTERNAL TABLE `company_base_business_merge_new` (
	company_name	STRING	DEFAULT NULL,
	company_regis_capital	STRING	DEFAULT NULL,
	company_currency	STRING	DEFAULT NULL,
	company_regis_code	STRING	DEFAULT NULL,
	company_credit_code	STRING	DEFAULT NULL,
	company_country_rating	STRING	DEFAULT NULL,
	company_organization_code	STRING	DEFAULT NULL,
	company_legal_name	STRING	DEFAULT NULL,
	company_opening_date	STRING	DEFAULT NULL,
	company_operat_state	STRING	DEFAULT NULL,
	company_registration_time	STRING	DEFAULT NULL,
	company_operat_begin_date	STRING	DEFAULT NULL,
	company_operat_end_date	STRING	DEFAULT NULL,
	company_industry	STRING	DEFAULT NULL,
	company_industry_code	STRING	DEFAULT NULL,
	company_type	STRING	DEFAULT NULL,
	company_area_code	STRING	DEFAULT NULL,
	province	STRING	DEFAULT NULL,
	company_registrate_authory	STRING	DEFAULT NULL,
	company_address	STRING	DEFAULT NULL,
	company_business_scope	STRING	DEFAULT NULL,
	company_final_annual_date	STRING	DEFAULT NULL,
	company_final_annual_year	STRING	DEFAULT NULL,
	company_logout_date	STRING	DEFAULT NULL,
	company_revocation_date	STRING	DEFAULT NULL,
	company_city_rating	STRING	DEFAULT NULL,
	company_name_en	STRING	DEFAULT NULL,
	gather_time	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL,
	info_chanle_id	STRING	DEFAULT NULL,
	company_name_histroy	STRING	DEFAULT NULL,
	rowkey	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_base_business_merge_new';


USE data_hub_new;
drop table if exists company_base_contact_info_new;
CREATE EXTERNAL TABLE `company_base_contact_info_new` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_address	STRING	DEFAULT NULL,
	company_business_model	STRING	DEFAULT NULL,
	company_company_size	STRING	DEFAULT NULL,
	company_contacts	STRING	DEFAULT NULL,
	company_email	STRING	DEFAULT NULL,
	company_fax_phone	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_postcode	STRING	DEFAULT NULL,
	company_telephone	STRING	DEFAULT NULL,
	company_web_site_url	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_base_contact_info_new';


USE data_hub_new;
drop table if exists company_bidding_informate;
CREATE EXTERNAL TABLE `company_bidding_informate` (
	rowkey	STRING	DEFAULT NULL,
	bidding_area	STRING	DEFAULT NULL,
	bidding_content	STRING	DEFAULT NULL,
	bidding_publishtime	STRING	DEFAULT NULL,
	bidding_purchaser	STRING	DEFAULT NULL,
	bidding_title	STRING	DEFAULT NULL,
	bidding_url	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_bidding_informate';


USE data_hub_new;
drop table if exists company_bond_info;
CREATE EXTERNAL TABLE `company_bond_info` (
	rowkey	STRING	DEFAULT NULL,
	bond_annual_rate	STRING	DEFAULT NULL,
	bond_annual_rate_change	STRING	DEFAULT NULL,
	bond_cashing_price	STRING	DEFAULT NULL,
	bond_circulate_range	STRING	DEFAULT NULL,
	bond_class	STRING	DEFAULT NULL,
	bond_code	STRING	DEFAULT NULL,
	bond_code_abbreviate	STRING	DEFAULT NULL,
	bond_create_place	STRING	DEFAULT NULL,
	bond_credit_level	STRING	DEFAULT NULL,
	bond_credit_org	STRING	DEFAULT NULL,
	bond_denomination	STRING	DEFAULT NULL,
	bond_due_date	STRING	DEFAULT NULL,
	bond_exercise_date	STRING	DEFAULT NULL,
	bond_exercise_type	STRING	DEFAULT NULL,
	bond_frequency	STRING	DEFAULT NULL,
	bond_interest_day	STRING	DEFAULT NULL,
	bond_interest_payment	STRING	DEFAULT NULL,
	bond_issue_amount	STRING	DEFAULT NULL,
	bond_issue_cautioner	STRING	DEFAULT NULL,
	bond_issue_end_time	STRING	DEFAULT NULL,
	bond_issue_object	STRING	DEFAULT NULL,
	bond_issue_price	STRING	DEFAULT NULL,
	bond_issue_start_time	STRING	DEFAULT NULL,
	bond_issue_way	STRING	DEFAULT NULL,
	bond_listing_day	STRING	DEFAULT NULL,
	bond_main_under_instit	STRING	DEFAULT NULL,
	bond_plan_issue_amount	STRING	DEFAULT NULL,
	bond_publish_time	STRING	DEFAULT NULL,
	bond_purchaser	STRING	DEFAULT NULL,
	bond_refer_rate	STRING	DEFAULT NULL,
	bond_remark	STRING	DEFAULT NULL,
	bond_spread	STRING	DEFAULT NULL,
	bond_tax_revenue	STRING	DEFAULT NULL,
	bond_term	STRING	DEFAULT NULL,
	bond_title	STRING	DEFAULT NULL,
	bond_value	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_bond_info';


USE data_hub_new;
drop table if exists company_branch_new;
CREATE EXTERNAL TABLE `company_branch_new` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	enterprise_name	STRING	DEFAULT NULL,
	enterprise_reg_no	STRING	DEFAULT NULL,
	father_company_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_branch_new';


USE data_hub_new;
drop table if exists company_chattel_mortgage;
CREATE EXTERNAL TABLE `company_chattel_mortgage` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_c_m_code	STRING	DEFAULT NULL,
	company_c_m_count	STRING	DEFAULT NULL,
	company_c_m_maturity	STRING	DEFAULT NULL,
	company_c_m_name	STRING	DEFAULT NULL,
	company_c_m_name_code	STRING	DEFAULT NULL,
	company_c_m_remarks	STRING	DEFAULT NULL,
	company_c_m_scope	STRING	DEFAULT NULL,
	company_c_m_status	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_m_name_code	STRING	DEFAULT NULL,
	company_m_type	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_regist_authory	STRING	DEFAULT NULL,
	company_regist_time	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_chattel_mortgage';


USE data_hub_new;
drop table if exists company_chattel_mort_coll;
CREATE EXTERNAL TABLE `company_chattel_mort_coll` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_c_m_code	STRING	DEFAULT NULL,
	company_c_m_remarks	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_m_thing	STRING	DEFAULT NULL,
	company_m_thing_ownership	STRING	DEFAULT NULL,
	company_m_thing_status	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_chattel_mort_coll';


USE data_hub_new;
drop table if exists company_check_public;
CREATE EXTERNAL TABLE `company_check_public` (
	rowkey	STRING	DEFAULT NULL,
	brand_name	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_credit_code	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_regis_code	STRING	DEFAULT NULL,
	execute_organizate_code	STRING	DEFAULT NULL,
	inspect_code	STRING	DEFAULT NULL,
	inspecte_unit	STRING	DEFAULT NULL,
	manufacture_code	STRING	DEFAULT NULL,
	model_size	STRING	DEFAULT NULL,
	product_name	STRING	DEFAULT NULL,
	province_name	STRING	DEFAULT NULL,
	randomresults	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL,
	type	STRING	DEFAULT NULL,
	unqualified_item	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_check_public';


USE data_hub_new;
drop table if exists company_comp_ent_produce;
CREATE EXTERNAL TABLE `company_comp_ent_produce` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	comp_ent_produce_area	STRING	DEFAULT NULL,
	comp_ent_produce_build_time	STRING	DEFAULT NULL,
	comp_ent_produce_business	STRING	DEFAULT NULL,
	comp_ent_produce_cur_round	STRING	DEFAULT NULL,
	comp_ent_produce_industry	STRING	DEFAULT NULL,
	comp_ent_produce_name	STRING	DEFAULT NULL,
	comp_ent_produce_valuation	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_comp_ent_produce';


USE data_hub_new;
drop table if exists company_custom_rating;
CREATE EXTERNAL TABLE `company_custom_rating` (
	rowkey	STRING	DEFAULT NULL,
	business_level	STRING	DEFAULT NULL,
	business_scope	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_url	STRING	DEFAULT NULL,
	industry_type	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_custom_rating';


USE data_hub_new;
drop table if exists company_environ_monitor;
CREATE EXTERNAL TABLE `company_environ_monitor` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	monitor_area_code	STRING	DEFAULT NULL,
	monitor_class	STRING	DEFAULT NULL,
	monitor_legal_name_code	STRING	DEFAULT NULL,
	monitor_province	STRING	DEFAULT NULL,
	monitor_year	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_environ_monitor';


USE data_hub_new;
drop table if exists company_envi_certify;
CREATE EXTERNAL TABLE `company_envi_certify` (
	rowkey	STRING	DEFAULT NULL,
	address	STRING	DEFAULT NULL,
	business_scope	STRING	DEFAULT NULL,
	certificate_status	STRING	DEFAULT NULL,
	certificate_type	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_url	STRING	DEFAULT NULL,
	manufacture_name	STRING	DEFAULT NULL,
	operating_period_end	STRING	DEFAULT NULL,
	permit_time	STRING	DEFAULT NULL,
	product_type	STRING	DEFAULT NULL,
	register_codes	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_envi_certify';


USE data_hub_new;
drop table if exists company_finance_overview;
CREATE EXTERNAL TABLE `company_finance_overview` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_f_o_pro_mar_clean	STRING	DEFAULT NULL,
	company_f_o_pro_mar_gross	STRING	DEFAULT NULL,
	company_f_o_strength_level	STRING	DEFAULT NULL,
	company_f_o_tax_range	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_finance_overview';


USE data_hub_new;
drop table if exists company_financing_info;
CREATE EXTERNAL TABLE `company_financing_info` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_finance_date	STRING	DEFAULT NULL,
	company_finance_investor	STRING	DEFAULT NULL,
	company_finance_level	STRING	DEFAULT NULL,
	company_finance_money	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_financing_info';


USE data_hub_new;
drop table if exists company_food_license;
CREATE EXTERNAL TABLE `company_food_license` (
	rowkey	STRING	DEFAULT NULL,
	address	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	code_old	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_url	STRING	DEFAULT NULL,
	credentials_code	STRING	DEFAULT NULL,
	detail	STRING	DEFAULT NULL,
	inspect_enterprise	STRING	DEFAULT NULL,
	issue_time	STRING	DEFAULT NULL,
	issuer	STRING	DEFAULT NULL,
	mark	STRING	DEFAULT NULL,
	period_validity	STRING	DEFAULT NULL,
	permit_enterprise	STRING	DEFAULT NULL,
	product_address	STRING	DEFAULT NULL,
	product_name	STRING	DEFAULT NULL,
	product_type	STRING	DEFAULT NULL,
	province	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL,
	superintendent	STRING	DEFAULT NULL,
	type_code	STRING	DEFAULT NULL,
	type_name	STRING	DEFAULT NULL,
	verify_mode	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_food_license';


USE data_hub_new;
drop table if exists company_gmp_gsp_license;
CREATE EXTERNAL TABLE `company_gmp_gsp_license` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_url	STRING	DEFAULT NULL,
	gmp_gsp_address	STRING	DEFAULT NULL,
	gmp_gsp_code	STRING	DEFAULT NULL,
	gmp_gsp_continue_scope	STRING	DEFAULT NULL,
	gmp_gsp_continue_time	STRING	DEFAULT NULL,
	gmp_gsp_continue_to	STRING	DEFAULT NULL,
	gmp_gsp_issue_time	STRING	DEFAULT NULL,
	gmp_gsp_province	STRING	DEFAULT NULL,
	gmp_gsp_record_time	STRING	DEFAULT NULL,
	gmp_gsp_remarks	STRING	DEFAULT NULL,
	gmp_gsp_scope	STRING	DEFAULT NULL,
	gmp_gsp_validity_time	STRING	DEFAULT NULL,
	gmp_gsp_version_gmp	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_gmp_gsp_license';


USE data_hub_new;
drop table if exists company_healfood_sampl;
CREATE EXTERNAL TABLE `company_healfood_sampl` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_check_addr	STRING	DEFAULT NULL,
	company_check_name	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_produce_addr	STRING	DEFAULT NULL,
	company_producer	STRING	DEFAULT NULL,
	inspect_brand_name	STRING	DEFAULT NULL,
	inspect_classificate	STRING	DEFAULT NULL,
	inspect_detail	STRING	DEFAULT NULL,
	inspect_identi_code	STRING	DEFAULT NULL,
	inspect_model_size	STRING	DEFAULT NULL,
	inspect_project	STRING	DEFAULT NULL,
	inspect_province	STRING	DEFAULT NULL,
	inspect_result	STRING	DEFAULT NULL,
	inspect_time	STRING	DEFAULT NULL,
	manufacture_code	STRING	DEFAULT NULL,
	product_name	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_healfood_sampl';


USE data_hub_new;
drop table if exists company_imp_exp_credit_info;
CREATE EXTERNAL TABLE `company_imp_exp_credit_info` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_annual_report	STRING	DEFAULT NULL,
	company_area	STRING	DEFAULT NULL,
	company_authenticate_code	STRING	DEFAULT NULL,
	company_cancellate_no	STRING	DEFAULT NULL,
	company_credit_level	STRING	DEFAULT NULL,
	company_customs_punish	STRING	DEFAULT NULL,
	company_e_commerce_class	STRING	DEFAULT NULL,
	company_economic_region	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_identificate_time	STRING	DEFAULT NULL,
	company_industry_class	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_oper_class	STRING	DEFAULT NULL,
	company_regis_code	STRING	DEFAULT NULL,
	company_regis_customs	STRING	DEFAULT NULL,
	company_regis_date	STRING	DEFAULT NULL,
	company_spe_trade_area	STRING	DEFAULT NULL,
	company_validity_customs	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_imp_exp_credit_info';


USE data_hub_new;
drop table if exists company_indus_license;
CREATE EXTERNAL TABLE `company_indus_license` (
	rowkey	STRING	DEFAULT NULL,
	address	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	credentials_code	STRING	DEFAULT NULL,
	inspect_type	STRING	DEFAULT NULL,
	issue_time	STRING	DEFAULT NULL,
	period_area	STRING	DEFAULT NULL,
	period_content	STRING	DEFAULT NULL,
	period_validity	STRING	DEFAULT NULL,
	permit_enterprise	STRING	DEFAULT NULL,
	product_address	STRING	DEFAULT NULL,
	product_name	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_indus_license';


USE data_hub_new;
drop table if exists company_ind_cred_record;
CREATE EXTERNAL TABLE `company_ind_cred_record` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_org_code	STRING	DEFAULT NULL,
	company_regis_code	STRING	DEFAULT NULL,
	credit_certifi_code	STRING	DEFAULT NULL,
	credit_certifi_time	STRING	DEFAULT NULL,
	credit_certifi_unit	STRING	DEFAULT NULL,
	credit_rating	STRING	DEFAULT NULL,
	credit_url	STRING	DEFAULT NULL,
	credit_validit_period	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_ind_cred_record';


USE data_hub_new;
drop table if exists company_land_info;
CREATE EXTERNAL TABLE `company_land_info` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	land_arrang_start_date	STRING	DEFAULT NULL,
	land_arrange_end_date	STRING	DEFAULT NULL,
	land_assignee	STRING	DEFAULT NULL,
	land_district	STRING	DEFAULT NULL,
	land_e_c_code	STRING	DEFAULT NULL,
	land_father_company	STRING	DEFAULT NULL,
	land_info_url	STRING	DEFAULT NULL,
	land_posite	STRING	DEFAULT NULL,
	land_purpose	STRING	DEFAULT NULL,
	land_signing_date	STRING	DEFAULT NULL,
	land_size	STRING	DEFAULT NULL,
	land_supply_mode	STRING	DEFAULT NULL,
	land_transacte_price	STRING	DEFAULT NULL,
	land_volume_limit	STRING	DEFAULT NULL,
	land_volume_lower	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_land_info';


USE data_hub_new;
drop table if exists company_no_special_cosmetics;
CREATE EXTERNAL TABLE `company_no_special_cosmetics` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_url	STRING	DEFAULT NULL,
	company_url_id	STRING	DEFAULT NULL,
	record_code	STRING	DEFAULT NULL,
	record_constituent	STRING	DEFAULT NULL,
	record_explained	STRING	DEFAULT NULL,
	record_prod_company	STRING	DEFAULT NULL,
	record_produces	STRING	DEFAULT NULL,
	record_publish_time	STRING	DEFAULT NULL,
	record_remarks	STRING	DEFAULT NULL,
	record_time	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_no_special_cosmetics';


USE data_hub_new;
drop table if exists company_operate_anomaly_new;
CREATE EXTERNAL TABLE `company_operate_anomaly_new` (
	rowkey	STRING	DEFAULT NULL,
	administrate_organ_name	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_id	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_reg_no	STRING	DEFAULT NULL,
	company_url	STRING	DEFAULT NULL,
	flag	STRING	DEFAULT NULL,
	include_resion	STRING	DEFAULT NULL,
	include_time	STRING	DEFAULT NULL,
	out_organ_name	STRING	DEFAULT NULL,
	out_resion	STRING	DEFAULT NULL,
	out_time	STRING	DEFAULT NULL,
	province	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_operate_anomaly_new';


USE data_hub_new;
drop table if exists company_produce;
CREATE EXTERNAL TABLE `company_produce` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_produce_calss	STRING	DEFAULT NULL,
	company_produce_index	STRING	DEFAULT NULL,
	company_produce_indus	STRING	DEFAULT NULL,
	company_produce_name	STRING	DEFAULT NULL,
	company_produce_remark	STRING	DEFAULT NULL,
	company_produce_sh_name	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_produce';


USE data_hub_new;
drop table if exists company_profit_statement;
CREATE EXTERNAL TABLE `company_profit_statement` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_p_s_assets_los	STRING	DEFAULT NULL,
	company_p_s_basic_eps	STRING	DEFAULT NULL,
	company_p_s_finace_exp	STRING	DEFAULT NULL,
	company_p_s_inco_compr	STRING	DEFAULT NULL,
	company_p_s_inco_parent	STRING	DEFAULT NULL,
	company_p_s_inco_total	STRING	DEFAULT NULL,
	company_p_s_inves_inco	STRING	DEFAULT NULL,
	company_p_s_manage_exp	STRING	DEFAULT NULL,
	company_p_s_net_profit	STRING	DEFAULT NULL,
	company_p_s_oper_cost	STRING	DEFAULT NULL,
	company_p_s_oper_inco	STRING	DEFAULT NULL,
	company_p_s_oper_prof	STRING	DEFAULT NULL,
	company_p_s_oper_taxe	STRING	DEFAULT NULL,
	company_p_s_per_shar_di	STRING	DEFAULT NULL,
	company_p_s_report_date	STRING	DEFAULT NULL,
	company_p_s_sale_expen	STRING	DEFAULT NULL,
	company_p_s_tax_income	STRING	DEFAULT NULL,
	company_p_s_total_pro	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_profit_statement';


USE data_hub_new;
drop table if exists company_secure_license;
CREATE EXTERNAL TABLE `company_secure_license` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_class_name	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_legal_name	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	secure_area	STRING	DEFAULT NULL,
	secure_code	STRING	DEFAULT NULL,
	secure_industry	STRING	DEFAULT NULL,
	secure_issue_time	STRING	DEFAULT NULL,
	secure_level	STRING	DEFAULT NULL,
	secure_pro_ability	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_secure_license';


USE data_hub_new;
drop table if exists company_senior_manager_new;
CREATE EXTERNAL TABLE `company_senior_manager_new` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_employee_name	STRING	DEFAULT NULL,
	company_employee_sex	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_id	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_posite	STRING	DEFAULT NULL,
	company_reg_no	STRING	DEFAULT NULL,
	company_url	STRING	DEFAULT NULL,
	flag	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_senior_manager_new';


USE data_hub_new;
drop table if exists company_soft_patent_new;
CREATE EXTERNAL TABLE `company_soft_patent_new` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	chanle_name	STRING	DEFAULT NULL,
	company	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_url	STRING	DEFAULT NULL,
	copyrigh_id	STRING	DEFAULT NULL,
	gather_resource_type	STRING	DEFAULT NULL,
	gather_time	STRING	DEFAULT NULL,
	group_id	STRING	DEFAULT NULL,
	last_updated_time	STRING	DEFAULT NULL,
	number_NEW	STRING	DEFAULT NULL,
	site_id	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL,
	software_appli_code	STRING	DEFAULT NULL,
	software_class_code	STRING	DEFAULT NULL,
	software_inventor	STRING	DEFAULT NULL,
	software_name	STRING	DEFAULT NULL,
	software_nickname	STRING	DEFAULT NULL,
	software_regist_code	STRING	DEFAULT NULL,
	software_version	STRING	DEFAULT NULL,
	success_date	STRING	DEFAULT NULL,
	template_parent_id	STRING	DEFAULT NULL,
	title	STRING	DEFAULT NULL,
	type_num	STRING	DEFAULT NULL,
	url	STRING	DEFAULT NULL,
	url_hash	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_soft_patent_new';


USE data_hub_new;
drop table if exists company_solid_waste_imp;
CREATE EXTERNAL TABLE `company_solid_waste_imp` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	solid_waste_app_total	STRING	DEFAULT NULL,
	solid_waste_batch	STRING	DEFAULT NULL,
	solid_waste_code	STRING	DEFAULT NULL,
	solid_waste_harbour	STRING	DEFAULT NULL,
	solid_waste_imp_com	STRING	DEFAULT NULL,
	solid_waste_mark	STRING	DEFAULT NULL,
	solid_waste_pro_name	STRING	DEFAULT NULL,
	solid_waste_pro_total	STRING	DEFAULT NULL,
	solid_waste_time	STRING	DEFAULT NULL,
	solid_waste_total	STRING	DEFAULT NULL,
	solid_waste_use_com	STRING	DEFAULT NULL,
	solid_waste_year	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_solid_waste_imp';


USE data_hub_new;
drop table if exists company_statement_cash_flow;
CREATE EXTERNAL TABLE `company_statement_cash_flow` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_s_c_f_cash_borrow	STRING	DEFAULT NULL,
	company_s_c_f_cash_debt_pay	STRING	DEFAULT NULL,
	company_s_c_f_cash_fi_in	STRING	DEFAULT NULL,
	company_s_c_f_cash_fina_inf	STRING	DEFAULT NULL,
	company_s_c_f_cash_fina_out	STRING	DEFAULT NULL,
	company_s_c_f_cash_fix_oth	STRING	DEFAULT NULL,
	company_s_c_f_cash_inf_in	STRING	DEFAULT NULL,
	company_s_c_f_cash_net_equi	STRING	DEFAULT NULL,
	company_s_c_f_cash_net_fina	STRING	DEFAULT NULL,
	company_s_c_f_cash_net_inv	STRING	DEFAULT NULL,
	company_s_c_f_cash_oper_ne	STRING	DEFAULT NULL,
	company_s_c_f_cash_outf_in	STRING	DEFAULT NULL,
	company_s_c_f_cash_pay_rais	STRING	DEFAULT NULL,
	company_s_c_f_cash_pr_inter	STRING	DEFAULT NULL,
	company_s_c_f_cash_rec_inv	STRING	DEFAULT NULL,
	company_s_c_f_cash_rec_oth	STRING	DEFAULT NULL,
	company_s_c_f_employ_pay	STRING	DEFAULT NULL,
	company_s_c_f_inc_cash	STRING	DEFAULT NULL,
	company_s_c_f_oper_cash_in	STRING	DEFAULT NULL,
	company_s_c_f_oper_cash_ou	STRING	DEFAULT NULL,
	company_s_c_f_tax_paid	STRING	DEFAULT NULL,
	company_s_c_f_tax_return	STRING	DEFAULT NULL,
	company_s_c_f_time	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_statement_cash_flow';


USE data_hub_new;
drop table if exists company_stock_info;
CREATE EXTERNAL TABLE `company_stock_info` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	shares_assis_name	STRING	DEFAULT NULL,
	shares_code	STRING	DEFAULT NULL,
	shares_contact_add	STRING	DEFAULT NULL,
	shares_email	STRING	DEFAULT NULL,
	shares_fax	STRING	DEFAULT NULL,
	shares_indus_code	STRING	DEFAULT NULL,
	shares_industry_code	STRING	DEFAULT NULL,
	shares_is_over_list	STRING	DEFAULT NULL,
	shares_is_share	STRING	DEFAULT NULL,
	shares_issue_price	STRING	DEFAULT NULL,
	shares_legal_name	STRING	DEFAULT NULL,
	shares_list_recommen	STRING	DEFAULT NULL,
	shares_main_underwr	STRING	DEFAULT NULL,
	shares_market_code	STRING	DEFAULT NULL,
	shares_nickname	STRING	DEFAULT NULL,
	shares_oversea_add	STRING	DEFAULT NULL,
	shares_post_code	STRING	DEFAULT NULL,
	shares_prospect_time	STRING	DEFAULT NULL,
	shares_province	STRING	DEFAULT NULL,
	shares_regis_add	STRING	DEFAULT NULL,
	shares_regis_code	STRING	DEFAULT NULL,
	shares_release_mode	STRING	DEFAULT NULL,
	shares_share_status	STRING	DEFAULT NULL,
	shares_sponsor_insti	STRING	DEFAULT NULL,
	shares_telephone	STRING	DEFAULT NULL,
	shares_type	STRING	DEFAULT NULL,
	shares_website	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_stock_info';


USE data_hub_new;
drop table if exists company_stock_notice;
CREATE EXTERNAL TABLE `company_stock_notice` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_nickname	STRING	DEFAULT NULL,
	notice_file_url	STRING	DEFAULT NULL,
	notice_id	STRING	DEFAULT NULL,
	notice_industry	STRING	DEFAULT NULL,
	notice_pdf_path	STRING	DEFAULT NULL,
	notice_plate	STRING	DEFAULT NULL,
	notice_publishtime	STRING	DEFAULT NULL,
	notice_shares_code	STRING	DEFAULT NULL,
	notice_title	STRING	DEFAULT NULL,
	notice_type	STRING	DEFAULT NULL,
	notice_url	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_stock_notice';


USE data_hub_new;
drop table if exists company_stock_right_target;
CREATE EXTERNAL TABLE `company_stock_right_target` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_stock_ple_data	STRING	DEFAULT NULL,
	company_stock_ple_number	STRING	DEFAULT NULL,
	company_stock_ple_status	STRING	DEFAULT NULL,
	company_stock_pledgee	STRING	DEFAULT NULL,
	company_stock_pledgor	STRING	DEFAULT NULL,
	company_stock_regis_no	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_stock_right_target';


USE data_hub_new;
drop table if exists company_tax_arrears_notice;
CREATE EXTERNAL TABLE `company_tax_arrears_notice` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_address	STRING	DEFAULT NULL,
	company_legal_name	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_tax_arrears_pros	STRING	DEFAULT NULL,
	company_tax_arrears_type	STRING	DEFAULT NULL,
	company_tax_card_code	STRING	DEFAULT NULL,
	company_tax_card_type	STRING	DEFAULT NULL,
	company_tax_code	STRING	DEFAULT NULL,
	company_tax_determ_time	STRING	DEFAULT NULL,
	company_tax_dire_dep	STRING	DEFAULT NULL,
	company_tax_end_time	STRING	DEFAULT NULL,
	company_tax_last_time	STRING	DEFAULT NULL,
	company_tax_manager	STRING	DEFAULT NULL,
	company_tax_pub_time	STRING	DEFAULT NULL,
	company_tax_start_time	STRING	DEFAULT NULL,
	company_tax_sum	STRING	DEFAULT NULL,
	company_tax_sum_cur	STRING	DEFAULT NULL,
	company_tax_type	STRING	DEFAULT NULL,
	company_type	STRING	DEFAULT NULL,
	gather_time	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_tax_arrears_notice';


USE data_hub_new;
drop table if exists company_tax_rating;
CREATE EXTERNAL TABLE `company_tax_rating` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	evaluate_year	STRING	DEFAULT NULL,
	grant_enterprise	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL,
	tax_level	STRING	DEFAULT NULL,
	taxpayer_code	STRING	DEFAULT NULL,
	taxpayer_type	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_tax_rating';


USE data_hub_new;
drop table if exists company_trademark;
CREATE EXTERNAL TABLE `company_trademark` (
	rowkey	STRING	DEFAULT NULL,
	address_cn	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company	STRING	DEFAULT NULL,
	company_en	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_url	STRING	DEFAULT NULL,
	created_time	STRING	DEFAULT NULL,
	element	STRING	DEFAULT NULL,
	end_date	STRING	DEFAULT NULL,
	global_date	STRING	DEFAULT NULL,
	image_name	STRING	DEFAULT NULL,
	is_shared	STRING	DEFAULT NULL,
	last_updated_time	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL,
	trademark_agent_name	STRING	DEFAULT NULL,
	trademark_appli_code	STRING	DEFAULT NULL,
	trademark_apply_name	STRING	DEFAULT NULL,
	trademark_apply_time	STRING	DEFAULT NULL,
	trademark_class_code	STRING	DEFAULT NULL,
	trademark_image_lopath	STRING	DEFAULT NULL,
	trademark_image_mark	STRING	DEFAULT NULL,
	trademark_image_weburl	STRING	DEFAULT NULL,
	trademark_name	STRING	DEFAULT NULL,
	trademark_preli_no	STRING	DEFAULT NULL,
	trademark_preli_time	STRING	DEFAULT NULL,
	trademark_priority_date	STRING	DEFAULT NULL,
	trademark_produce_name	STRING	DEFAULT NULL,
	trademark_reg_time	STRING	DEFAULT NULL,
	trademark_regist_no	STRING	DEFAULT NULL,
	trademark_regist_time	STRING	DEFAULT NULL,
	trademark_specif_color	STRING	DEFAULT NULL,
	trademark_status	STRING	DEFAULT NULL,
	trademark_type	STRING	DEFAULT NULL,
	trademark_valid_time	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_trademark';


USE data_hub_new;
drop table if exists company_web_filings_new;
CREATE EXTERNAL TABLE `company_web_filings_new` (
	rowkey	STRING	DEFAULT NULL,
	audit_time	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_index	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	host_unit_nature	STRING	DEFAULT NULL,
	record_status	STRING	DEFAULT NULL,
	register_code	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL,
	website_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_web_filings_new';


USE data_hub_new;
drop table if exists company_works_patent;
CREATE EXTERNAL TABLE `company_works_patent` (
	rowkey	STRING	DEFAULT NULL,
	chanle_id	STRING	DEFAULT NULL,
	company_gather_time	STRING	DEFAULT NULL,
	company_name	STRING	DEFAULT NULL,
	company_works_code	STRING	DEFAULT NULL,
	company_works_com_time	STRING	DEFAULT NULL,
	company_works_first_pub	STRING	DEFAULT NULL,
	company_works_name	STRING	DEFAULT NULL,
	company_works_reg_time	STRING	DEFAULT NULL,
	company_works_type	STRING	DEFAULT NULL,
	site_name	STRING	DEFAULT NULL)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LOCATION '/tmp/20180208/company_works_patent';


