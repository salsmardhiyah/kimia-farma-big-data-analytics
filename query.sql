-- Data Preparation --
-- Create new schema -- 
CREATE SCHEMA IF NOT EXISTS data;

-- Create table from dataset --
CREATE TABLE IF NOT EXISTS data.barang
(
	kode_barang varchar(7) PRIMARY KEY,
	sektor varchar(3),
	nama_barang varchar(50),
	tipe varchar(4),
	nama_tipe varchar(20),
	kode_lini smallint,
	lini varchar(20),
	kemasan varchar(10)
);
CREATE TABLE IF NOT EXISTS data.pelanggan
(
	id_customer varchar(9),
	"level" varchar(10),
	nama varchar(30),
	id_cabang_sales varchar(5),
	cabang_sales varchar(50),
	id_group varchar(3),
	"group" varchar(20)
);

CREATE TABLE IF NOT EXISTS data.penjualan 
(
	id_distributor varchar(5),
	id_cabang varchar(5),
	id_invoice varchar(6) PRIMARY KEY,
	tanggal date,
	id_customer varchar(9) references pelanggan(id_customer),
	id_barang varchar(7) references barang(kode_barang),
	jumlah_barang numeric,
	unit varchar(10),
	harga numeric,
	mata_uang varchar(3),
	brand_id varchar(7),
	lini varchar(20)
);

-- Change Date Style --
ALTER DATABASE kimia_farma
    SET "DateStyle" TO 'ISO, DMY';

-- Create Base Table --
CREATE TABLE data.base_table AS 
	SELECT 
		pj.id_invoice,
		pj.tanggal,
		pl.cabang_sales,
		pl.nama nama_customer,
		pl.group,
		b.nama_barang,
		b.lini brand,
		pj.jumlah_barang,
		pj.unit,
		pj.harga
	FROM
		data.penjualan pj
		JOIN data.pelanggan pl
			ON pl.id_customer = pj.id_customer
		JOIN data.barang b
			ON pj.id_barang = b.kode_barang
;

-- Create Aggregate Table --
CREATE TABLE data.aggregate_table AS
	SELECT
		id_invoice,
		tanggal,
		TO_CHAR(tanggal, 'Mon') bulan, ---Extract month name from date
		cabang_sales,
		nama_customer,
		"group",
		nama_barang,
		brand,
		jumlah_barang,
		unit,
		harga,
		ROUND(jumlah_barang * harga) total_harga ---calculate and round total price
	FROM
		data.base_table
;

-- Create Aggregate Table 2: Monthly Sales Summary --
CREATE TABLE data.monthly_sales AS
	SELECT
		EXTRACT(MONTH FROM tanggal) bulan_num,
		bulan,
		cabang_sales,
		nama_customer,
		nama_barang,
		brand,
		SUM(jumlah_barang) total_qty,
		SUM(total_harga) total_sales,
		SUM(SUM(total_harga)) OVER(PARTITION BY bulan, cabang_sales) branch_total_monthly_sales
	FROM
		data.aggregate_table
	GROUP BY
		1,2, 3, 4, 5, 6
	ORDER BY
		1,3,5
;

-- Create Aggregate Table 3: Month-over-Month Sales Performance --
CREATE TABLE data.mom_growth_rate AS
	SELECT
		DATE_TRUNC('month', tanggal) AS month,
		SUM(total_harga) AS total_sales,
		LAG(SUM(total_harga)) OVER (ORDER BY DATE_TRUNC('month', tanggal)) AS prev_month_sales,
		(SUM(total_harga) - LAG(SUM(total_harga)) OVER (ORDER BY DATE_TRUNC('month', tanggal))) AS monthly_growth,
		CAST(ROUND((SUM(total_harga) - LAG(SUM(total_harga)) OVER (ORDER BY DATE_TRUNC('month', tanggal))) 
			/ LAG(SUM(total_harga)) OVER (ORDER BY DATE_TRUNC('month', tanggal)) * 100) AS text)
			||'%' AS mom_growth_rate
	FROM
	  data.aggregate_table
	GROUP BY
	  1
	ORDER BY
	  DATE_TRUNC('month', tanggal)
;

