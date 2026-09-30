-- Gitlytics warehouse database (created once by setup/install_hive.sh as well)
CREATE DATABASE IF NOT EXISTS gitlytics
COMMENT 'Gitlytics: GitHub event analytics (bronze, silver, gold)';
