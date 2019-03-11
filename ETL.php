<?php



function import(&$pdo, $csv_path, $options = array())
{
	extract($options);
	
	if (($csv_handle = fopen($csv_path, "r")) === false) {
		throw new Exception('Cannot open CSV file');
	}

	if (!$delimiter) {
		$delimiter = ',';
	}
	if(!$table) {
		$table = preg_replace("/[^A-Z0-9]/i", '', basename($csv_path));
	}

	if(!$fields){
		$fields = array_map(function ($field){
			return strtolower(preg_replace("/[^A-Z0-9]/i", '_', $field));
		}, fgetcsv($csv_handle, 0, $delimiter));
	}
	
    $create_fields_str = create_fields($fields);
	
	$pdo->beginTransaction();
	
	echo "\n Importing csv ". substr($csv_path, 0, 50).'  to table: "'.$table.'" ';

	
	create_table($pdo, $table, $create_fields_str);

	$insert_sql = create_insert_table($table, $fields);
	
	$insert_sth = $pdo->prepare($insert_sql);
	
	$inserted_rows = 0;

	while (($data = fgetcsv($csv_handle, 0, $delimiter)) !== false) {
		$insert_sth->execute($data);
		$inserted_rows++;
	}
	
	$pdo->commit();
	
	fclose($csv_handle);
	
	echo " ...Done. Imported {$inserted_rows} rows! \n";


	return array(
		'table' => $table,
		'fields' => $fields,
		'insert' => $insert_sth,
		'inserted_rows' => $inserted_rows
	);
	
}


function create_insert_table($table, $fields)
{
	$insert_fields_str = join(', ', $fields);
	$insert_values_str = join(', ', array_fill(0, count($fields),  '?'));
	$insert_sql = "INSERT INTO $table ($insert_fields_str) VALUES ($insert_values_str)";
	return $insert_sql;
}

function create_fields($fields)
{
	return join(', ', array_map(function ($field){
		
		if (strpos($field, 'date') !== false) {
			return "$field DATETIME NULL";
		}

		return "$field VARCHAR NULL";
	}, $fields));
}

function create_table(&$pdo, $table, $create_fields_str, $options=[])
{
	extract($options);

	if (!isset($drop)) {
		$drop = true;
	}
	
	if ($drop) {
		$create_table_sql = "DROP TABLE IF EXISTS $table";
		$stmt = $pdo->exec($create_table_sql);

		if (!$stmt) {
			handle_error($pdo);
		}

	}

	$create_table_sql = "CREATE TABLE IF NOT EXISTS $table ($create_fields_str)";
	$stmt = $pdo->exec($create_table_sql);

	if (!$stmt) {
		handle_error($pdo);
	}

}

function handle_error(&$pdo)
{	
	$err = $pdo->errorInfo();

	if (isset($err[2]) && $err[2]) {
		print_r($err);
		exit;
	}

}


function create_csv_file($fileName)
{
	return fopen("./".$fileName, "w+");
}

function close_csv_file(&$fp)
{
	fclose($fp);
}

function write_csv_file(&$fp, $ccsv, $options=[])
{
	extract($options);

	if(!isset($delimiter)) {
		$delimiter = '~';
	}

	fputcsv($fp, $ccsv, $delimiter);
}


function export(&$pdo, $sql, $table, $options=[])
{
	extract($options);

	if (!isset($exportFile)) {
		$exportFile =  true;
	}

	if (!isset($exportDB)) {
		$exportDB = true;
	}

	if ($exportDB) {
		$pdo->beginTransaction();
	}

	if ($exportFile) {
		$fp = create_csv_file($table.'.txt');
	}
	
	$queryResults = $pdo->query($sql);
	
	if (!$queryResults) {
		handle_error($pdo);
	}

	echo "\n Exporting ". substr($sql, 0, 50).'...'.' with to "'.$table.'" ';

	if($queryResults != null) {

		$row = $queryResults->fetch(PDO::FETCH_ASSOC);

		if (!empty($row)) {
			$headers = array_keys($row);
		} elseif (isset($headers)) {
			$headers = explode(',', $headers);
		} else {
			die ("\n *** Export operation not possible since no rows are selected/headers provided! \n");
		}

		$queryResults = null;

		if ($exportDB) {
			$create_fields_str = create_fields($headers);
			create_table($pdo, $table, $create_fields_str);

			$insert_sql = create_insert_table($table, $headers);
			$insert_sth = $pdo->prepare($insert_sql);
		}

		$header = true;

		if ($exportFile) {
			write_csv_file($fp, $headers);
		}

		
	}

	$queryResults = $pdo->query($sql);
	while ($data = $queryResults->fetch(PDO::FETCH_NUM)) {
		if ($exportDB) {
			$insert_sth->execute($data);
		}
		if ($exportFile) {
			write_csv_file($fp, $data);
		}
	}

	if ($exportFile) {
		close_csv_file($fp);
	}

	if ($exportDB) {
		$pdo->commit();
	}

	echo " ... Done \n";

	//print_r($rows);exit;

	//return $rows;  
}
	
function generate_pdo($fileName, $options=[])
{

	extract($options);
	
	if (!isset($unlink)) {
		$unlink = false;
	}

	if ($unlink) {
		@unlink($fileName.'.db');
	}

	return new \PDO("sqlite:" . $fileName.'.db');
}

function split_files($fname, $limit = 5000)
{
	$limit = 5000;
	
	$export = 'split_files';

	@mkdir($export);

	$handle = fopen($fname,'r'); 

	$header = '';

	$f = 1; //new file number
	while(!feof($handle))
	{
		$headerWrite = true;
		
		$newfile = fopen("./{$export}/".$fname.$f.'.txt', 'w');

		for($i = 1; $i <= $limit; $i++) 
		{
			$import = fgets($handle);

			if ($i==1 && !$header) {
				$header = $import;
				continue;
			}

			if ($headerWrite) {
				fwrite($newfile, $header);
				$headerWrite = false;
			}

			fwrite($newfile, $import);
			if(feof($handle))
			{break;} //If file ends, break loop
		}

		fclose($newfile);

		$f++; //Increment newfile number
	}
	fclose($handle);
}


function convert_json_to_csv($json, $csvfile)
{
	$ch = create_csv_file($csvfile);

	foreach($json as $js) {
		write_csv_file($ch, array_values($js));
	}
	
	close_csv_file($ch);
}
