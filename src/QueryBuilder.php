<?php namespace Model\QueryBuilder;

use Model\DbParser\Parser;
use Model\DbParser\Table;

class QueryBuilder
{
	public function __construct(private Parser $parser)
	{
	}

	/**
	 * @return \PDO
	 */
	private function getDb(): \PDO
	{
		return $this->parser->getDb();
	}

	/**
	 * @param string $table
	 * @param array $data
	 * @param array $options
	 * @return string
	 */
	public function insert(string $table, array $data = [], array $options = []): string
	{
		$options = array_merge([
			'replace' => false,
		], $options);

		$qry_init = $options['replace'] ? 'REPLACE' : 'INSERT';

		if (empty($data)) {
			return $qry_init . ' INTO `' . $table . '`() VALUES()';
		} else {
			$this->validateData($table, $data);

			$qryStr = $this->buildQueryString($data, [
				'table' => $table,
				'glue' => ',',
				'for-select' => false,
			]);

			return $qry_init . ' INTO `' . $table . '` SET ' . $qryStr;
		}
	}

	/**
	 * @param string $table
	 * @param array|int $where
	 * @param array $data
	 * @return string|null
	 */
	public function update(string $table, array|int $where = [], array $data = []): ?string
	{
		if (empty($data))
			return null;

		$whereStr = $this->buildQueryString($where, ['table' => $table]);

		$dataStr = $this->buildQueryString($data, [
			'table' => $table,
			'glue' => ',',
			'for-select' => false,
		]);

		$qry = 'UPDATE `' . $table . '` SET ' . $dataStr;
		if ($whereStr)
			$qry .= ' WHERE ' . $whereStr;

		return $qry;
	}

	/**
	 * @param string $table
	 * @param array|int $where
	 * @return string
	 */
	public function delete(string $table, array|int $where = []): string
	{
		$whereStr = $this->buildQueryString($where, ['table' => $table]);

		$qry = 'DELETE FROM `' . $table . '`';
		if ($whereStr)
			$qry .= ' WHERE ' . $whereStr;

		return $qry;
	}

	/**
	 * @param array|int $where
	 * @param array $options
	 * @return string
	 */
	private function buildQueryString(array|int $where, array $options = []): string
	{
		if (!is_array($where))
			$where = ['id' => $where];

		$options = array_merge([
			'table' => null,
			'alias' => null,
			'glue' => 'AND',
			'for-select' => true,
			'joins' => [],
		], $options);

		$tableModel = null;
		if ($options['table'])
			$tableModel = $this->parser->getTable($options['table']);

		$str = [];

		foreach ($where as $k => $item) {
			$substr = null;

			if (is_array($item)) {
				if (in_array(strtoupper($k), ['OR', 'AND'])) {
					$substr = $this->buildQueryString($item, array_merge($options, ['glue' => strtoupper($k)]));
				} else {
					switch (count($item)) {
						case 1:
							if (in_array(strtoupper(array_key_first($item)), ['OR', 'AND']))
								$substr = $this->buildQueryString(reset($item), array_merge($options, ['glue' => strtoupper(array_key_first($item))]));
							else
								throw new \Exception('Query build error: wrong items number in array #1');
							break;
						case 2:
							if (is_numeric($k)) {
								if (in_array(strtoupper($item[0]), ['OR', 'AND'])) {
									if (!is_array($item[1]))
										throw new \Exception('Operator "' . $item[0] . '" needs an array');

									$substr = $this->buildQueryString($item[1], array_merge($options, ['glue' => strtoupper($item[0])]));
								} elseif (isset($item['sub'], $item['operator'])) {
									if (!in_array(strtoupper($item['operator']), ['OR', 'AND']))
										throw new \Exception('Operator "' . $item['operator'] . '" not supported');

									$substr = $this->buildQueryString($item['sub'], array_merge($options, ['glue' => strtoupper($item['operator'])]));
								} else {
									$column = $item[0];
									$operator = '=';
									$value = $item[1];
								}
							} else {
								$column = $k;

								if ($tableModel and is_string($column) and isset($tableModel->columns[$column]) and $tableModel->columns[$column]['type'] === 'point') {
									$operator = '=';
									$value = $item;
								} else {
									$operator = $item[0];
									$value = $item[1];
								}
							}
							break;
						case 3:
							$column = $item[0];
							$operator = $item[1];
							$value = $item[2];
							break;
						default:
							throw new \Exception('Query build error: wrong items number in array #2');
					}
				}
			} else {
				if (is_numeric($k)) {
					if (is_string($item))
						$substr = $item;
					else
						throw new \Exception('Query build error: bad item type');
				} else {
					$column = $k;
					$operator = '=';
					$value = $item;
				}
			}

			if ($substr !== null) {
				$str[] = '(' . $substr . ')';
			} else {
				$operator = strtoupper($operator);

				$columnType = null;
				if ($operator === 'MATCH') {
					if (!is_array($column)) {
						if (is_string($column))
							$column = [$column];
						else
							throw new \Exception('Column name must be a string');
					}

					$parsedColumnArr = [];
					foreach ($column as $c) {
						if ($tableModel and !isset($tableModel->columns[$c]))
							throw new \Exception('Column "' . $c . '" does not exist in table "' . $options['table'] . '"');

						$parsedColumnArr[] = $this->parseColumn($c, [
							'table' => $options['alias'] ?? $options['table'],
							'joins' => $options['joins'],
						]);
					}

					$parsedColumn = implode(',', $parsedColumnArr);
				} else {
					if (is_string($column))
						throw new \Exception('Column name must be a string');

					if ($tableModel) {
						if (!isset($tableModel->columns[$column]))
							throw new \Exception('Column "' . $column . '" does not exist in table "' . $options['table'] . '"');

						$columnType = $tableModel->columns[$column]['type'];
					}

					$parsedColumn = $this->parseColumn($column, [
						'table' => $options['alias'] ?? $options['table'],
						'joins' => $options['joins'],
					]);
				}

				if ($value === null and $options['for-select']) {
					switch ($operator) {
						case '=':
							$operator = 'IS';
							break;
						case '!=':
							$operator = 'IS NOT';
							break;
						default:
							throw new \Exception('Query build error: bad operator for null value');
					}
				}

				switch ($operator) {
					case 'BETWEEN':
						if (!is_array($value) or count($value) !== 2)
							throw new \Exception('"between" expects an array of 2 elements');

						if ($tableModel) {
							$this->validateColumnValue($tableModel, $column, $value[0]);
							$this->validateColumnValue($tableModel, $column, $value[1]);
						}

						$substr = $parsedColumn . ' BETWEEN ' . $this->parseValue($value[0], $columnType) . ' AND ' . $this->parseValue($value[1], $columnType);
						break;
					case 'IN':
					case 'NOT IN':
						if (!is_array($value))
							throw new \Exception('"in" or "not in" expect an array');

						if (count($value) === 0) {
							switch ($operator) {
								case 'IN': // If "IN" with an empty array is requested, I can\'t execute the query, so I will put an impossible condition
									$substr = '(1=2)';
									break;
								case 'NOT IN':
									continue 3;
							}
						} else {
							$parsedValues = [];
							foreach ($value as $v) {
								if ($tableModel)
									$this->validateColumnValue($tableModel, $column, $v);
								$parsedValues[] = $this->parseValue($v, $columnType);
							}

							$substr = $parsedColumn . ' ' . $operator . ' (' . implode(',', $parsedValues) . ')';
						}
						break;
					case 'MATCH':
						$substr = 'MATCH(' . $k . ') AGAINST(' . $this->parseValue($value) . ')';
						break;
					default:
						if ($tableModel)
							$this->validateColumnValue($tableModel, $column, $value);

						$substr = $parsedColumn . ' ' . $operator . ' ' . $this->parseValue($value, $columnType);
						break;
				}

				$str[] = $substr;
			}
		}

		return implode(' ' . $options['glue'] . ' ', $str);
	}

	/**
	 * @param string $k
	 * @param array $options
	 * @return string
	 */
	public function parseColumn(string $k, array $options = []): string
	{
		$k = preg_replace('/[^a-zA-Z0-9_.,()!=<> -]+/', '', $k);
		if (str_contains($k, '.')) {
			$k = explode('.', $k);
			return '`' . $k[0] . '`.`' . $k[1] . '`';
		} else {
			$options = array_merge([
				'table' => null,
				'joins' => [],
			], $options);

			$table = $options['table'];

			foreach ($options['joins'] as $join) {
				$joinedTable = $join['alias'] ?? $join['table'];
				foreach (($join['fields'] ?? []) as $fieldIdx => $field) {
					$fieldName = is_numeric($fieldIdx) ? $field : $fieldIdx;

					if ($field === $k) {
						$table = $joinedTable;
						$k = $fieldName;
						break 2;
					}
				}
			}

			return $table ? '`' . $table . '`.`' . $k . '`' : '`' . $k . '`';
		}
	}

	/**
	 * @param mixed $v
	 * @param string|null $type
	 * @return string
	 * @throws \Exception
	 */
	public function parseValue(mixed $v, ?string $type = null): string
	{
		if ($v === null)
			return 'NULL';

		if (is_object($v)) {
			if (($type === null or in_array($type, ['date', 'datetime', 'time'])) and get_class($v) === 'DateTime')
				$v = $v->format('Y-m-d H:i:s');
			else
				throw new \Exception('Only date/time can be passed as object as db values');
		}

		if (is_array($v)) {
			if ($type === 'point')
				return 'POINT(' . $v[0] . ',' . $v[1] . ')';
			else
				throw new \Exception('Db error: unknown value type in query (' . print_r($v, true) . ')');
		} else {
			return $this->getDb()->quote($v);
		}
	}

	/**
	 * @param string $table
	 * @param array $data
	 * @return void
	 */
	private function validateData(string $table, array $data): void
	{
		$tableModel = $this->parser->getTable($table);
		foreach ($data as $k => $v)
			$this->validateColumnValue($tableModel, $k, $v);
	}

	/**
	 * @param Table $table
	 * @param string $columnName
	 * @param mixed $v
	 * @return void
	 * @throws \Exception
	 */
	private function validateColumnValue(Table $table, string $columnName, mixed $v): void
	{
		if (!array_key_exists($columnName, $table->columns))
			throw new \Exception('Database column "' . $table->name . '.' . $columnName . '" does not exist!');

		$column = $table->columns;
		if ($v === null) {
			if (!$column['null'])
				throw new \Exception('"' . $table->name . '.' . $columnName . '" cannot be null');

			return;
		}

		switch ($column['type']) {
			case 'int':
			case 'tinyint':
			case 'smallint':
			case 'mediumint':
			case 'bigint':
			case 'float':
			case 'decimal':
			case 'double':
			case 'year':
				if (!is_numeric($v))
					throw new \Exception('"' . $table->name . '.' . $columnName . '" must be numeric');
				break;

			case 'char':
			case 'varchar':
				if ($column['length'] and strlen($v) > $column['length'])
					throw new \Exception('"' . $table->name . '.' . $columnName . '" length exceeded (must be shorter than ' . $column['length'] . ')');
				break;

			case 'date':
			case 'datetime':
				if (is_object($v) and get_class($v) == 'DateTime')
					$checkData = $v;
				else
					$checkData = $v ? date_create($v) : null;

				if (!$checkData)
					throw new \Exception('"' . $table->name . '.' . $columnName . '": bad date format');
				break;

			case 'point':
				if (!is_array($v) or count($v) !== 2 or !is_numeric($v[0]) or !is_numeric($v[1]))
					throw new \Exception('"' . $table->name . '.' . $columnName . '": bad point format');
				break;
		}
	}
}
