docker run --name oracle -d -it -P  -p 1521:1521  -p 5500:5500 -e ORACLE_SID=ORCLCDB -e ORACLE_PDB=ORCLPDB1 store/oracle/database-enterprise:12.2.0.1

