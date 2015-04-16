DROP TABLE IF EXISTS formulas;
CREATE TABLE formulas (
	id		text,
	name	text,
	sf 		text
);
\COPY formulas FROM '/home/snikolenko/soft/ims/dump.csv' WITH delimiter ';' quote '@' csv;

DROP TABLE IF EXISTS datasets;
CREATE TABLE datasets (
	dataset_id	int,
	dataset		text,
	filename	text,
	nrows		int,
	ncols		int
);
INSERT INTO datasets VALUES (0, 'test', '/media/data/ims/testdataset.txt', 51, 49);
INSERT INTO datasets VALUES (1, 'Ctrl3s2', '/media/data/ims/Ctrl3s2_SpheroidsCtrl_DHBSub_IMS.txt', 51, 49);
INSERT INTO datasets VALUES (2, 'Rat_brain', '/media/data/ims/Rat_brain_1M_50um_centroids_IMS.txt', 51, 49);
-- INSERT INTO datasets VALUES (0, 'test', '/home/snikolenko/soft/ims/data/testdataset.txt', 51, 49);
-- INSERT INTO datasets VALUES (1, 'Ctrl3s2', '/home/snikolenko/soft/ims/data/Ctrl3s2_SpheroidsCtrl_DHBSub_IMS.txt', 51, 49);
-- INSERT INTO datasets VALUES (2, 'Rat_brain', '/home/snikolenko/soft/ims/data/Rat_brain_1M_50um_centroids_IMS.txt', 51, 49);


DROP TABLE IF EXISTS job_types;
CREATE TABLE job_types (
	type 		int,
	description	text
);
INSERT INTO job_types VALUES (0, 'Extracting m/z values');
INSERT INTO job_types VALUES (1, 'Full dataset m/z image extraction');

DROP TABLE IF EXISTS jobs;
CREATE TABLE jobs (
	id 			int,
	type		int,
	formula_id	text,
	dataset_id	int,
	done		boolean,
	status		text,
	tasks_done	int,
	tasks_total	int,
	start       timestamp,
	finish      timestamp
);
-- \COPY jobs FROM '/home/snikolenko/soft/ims/pySIN/frontend/jobs.csv' WITH delimiter ';' quote '@' csv;

DROP TABLE IF EXISTS job_result_data;
CREATE TABLE job_result_data (
	job_id			int,
	param			int,
	peak			int,
	spectrum		int,
	value real
);
CREATE INDEX ind_job_result_data_1 ON job_result_data (job_id);
CREATE INDEX ind_job_result_data_2 ON job_result_data (job_id, param);

DROP TABLE IF EXISTS job_result_stats;
CREATE TABLE job_result_stats (
	job_id			int,
	formula_id		text,
	param			int,
	stats 			json
);


DROP TABLE IF EXISTS mz_peaks;
CREATE TABLE mz_peaks (
	formula_id		text,
	peaks			real[]
);
\COPY mz_peaks FROM '/home/snikolenko/soft/ims/pySIN/frontend/mzpeaks.csv' WITH delimiter ';' csv;
