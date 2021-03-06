<?php namespace Model\QueryBuilder;

use Model\DbParser\Parser;
use Model\DbParser\Table;

class QueryBuilder
{
	public function __construct(private readonly Parser $parser)
	{
	}

	/**
	 * @return Parser
	 */
	private function getParser(): Parser
	{
		return $this->parser;
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
	 * @param string $table
	 * @param array|int $where
	 * @param array $options
	 * @return string
	 */
	public function select(string $table, array|int $where = [], array $options = []): string
	{
		$options = array_merge([
			'alias' => null,
			'joins' => [],
			'fields' => null,
			'group_by' => null,
			'order_by' => null,
			'limit' => null,
		], $options);

		$options['joins'] = $this->normalizeJoins($options['alias'] ?? $table, $options['joins']);

		$whereStr = $this->buildQueryString($where, [
			'table' => $table,
			'alias' => $options['alias'],
			'joins' => $options['joins'],
		]);

		$joinStr = $this->buildJoins($options['joins']);

		$tableModel = $this->parser->getTable($table);

		$fields_str = [];
		if ($options['fields']) {
			if (is_array($options['fields'])) {
				foreach ($options['fields'] as $field) {
					if (!isset($tableModel->columns[$field]))
						throw new \Exception('Field "' . $field . '" does not exist');
					if ($tableModel->columns[$field]['type'] === 'point') {
						$parsedField = $this->parseColumn($field, ['table' => $options['alias'] ?? $table]);
						$fields_str[] = 'ST_AsText(' . $parsedField . ') AS ' . $this->parseColumn($field);
					} else {
						$fields_str[] = $this->parseColumn($field, ['table' => $options['alias'] ?? $table]);
					}
				}
			} elseif (is_string($options['fields'])) {
				$fields_str = [$options['fields']];
			} else {
				throw new \Exception('Error while building select query, "fields" must be either an array or a string');
			}
		} else {
			$fields_str[] = '*';
			foreach ($tableModel->columns as $field => $fieldOpt) {
				if ($fieldOpt['type'] === 'point') {
					$parsedField = $this->parseColumn($field, ['table' => $options['alias'] ?? $table]);
					$fields_str[] = 'ST_AsText(' . $parsedField . ') AS ' . $this->parseColumn($field);
				}
			}
		}

		$fields_str = implode(',', $fields_str);

		$fields_from_joins = [];
		foreach ($options['joins'] as $join) {
			foreach ($join['fields'] as $fieldIdx => $field) {
				$tableName = $join['alias'] ?? $join['table'];
				if (is_numeric($fieldIdx))
					$fields_from_joins[] = $this->parseColumn($field, ['table' => $tableName]);
				else
					$fields_from_joins[] = $this->parseColumn($fieldIdx, ['table' => $tableName]) . ' AS ' . $this->parseColumn($field);
			}
		}

		if ($fields_from_joins) {
			if (str_starts_with($fields_str, '*'))
				$fields_str = '`' . $table . '`.' . $fields_str;
			$fields_str .= ',' . implode(',', $fields_from_joins);
		}

		$qry = 'SELECT ' . $fields_str . ' FROM `' . $table . '`' . $joinStr;
		if ($whereStr)
			$qry .= ' WHERE ' . $whereStr;

		if ($options['group_by'] !== null) {
			if (!is_array($options['group_by']))
				$options['group_by'] = [$options['group_by']];

			foreach ($options['group_by'] as &$field) {
				$field = $this->parseColumn($field, [
					'table' => $options['alias'] ?? $table,
					'joins' => $options['joins'],
				]);
			}
			unset($field);

			$qry .= ' GROUP BY ' . implode(',', $options['group_by']);
		}

		if ($options['order_by'] !== null) {
			if (is_array($options['order_by'])) {
				foreach ($options['order_by'] as &$sortingField) {
					if (!is_array($sortingField))
						$sortingField = [$sortingField, 'ASC'];
					if (!in_array(strtoupper($sortingField[1]), ['ASC', 'DESC']))
						throw new \Exception('Bad "order by" direction');

					$sortingField[0] = $this->parseColumn($sortingField[0], [
						'table' => $options['alias'] ?? $table,
						'joins' => $options['joins'],
					]);
					$sortingField = implode(' ', $sortingField);
				}
				unset($sortingField);

				$qry .= ' ORDER BY ' . implode(',', $options['order_by']);
			} else {
				$qry .= ' ORDER BY ' . $options['order_by'];
			}
		}

		if ($options['limit'] !== null)
			$qry .= ' LIMIT ' . $options['limit'];

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
				$tableModelForValidate = $tableModel;

				if ($operator === 'MATCH') {
					if (!is_array($column)) {
						if (is_string($column))
							$column = [$column];
						else
							throw new \Exception('Column name must be a string');
					}

					$parsedColumnArr = [];
					foreach ($column as $c) {
						$parsedColumnArr[] = $this->parseColumn($c, [
							'table' => $options['alias'] ?? $options['table'],
							'joins' => $options['joins'],
						]);
					}

					$parsedColumn = implode(',', $parsedColumnArr);
				} else {
					if (!is_string($column))
						throw new \Exception('Column name must be a string');

					foreach ($options['joins'] as $join) {
						foreach ($join['fields'] as $fieldIdx => $field) {
							if ($field === $k) {
								$joinedTableModel = $this->parser->getTable($join['table']);
								$realColumn = is_numeric($fieldIdx) ? $field : $fieldIdx;
								if (!isset($joinedTableModel->columns[$realColumn]))
									throw new \Exception('Column "' . $realColumn . '" does not exist in table "' . $join['table'] . '"');

								$tableModelForValidate = $joinedTableModel;
								$columnType = $joinedTableModel->columns[$column]['type'];
								break 2;
							}
						}
					}

					if ($columnType === null and $tableModel) {
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
							$this->validateColumnValue($tableModelForValidate, $column, $value[0]);
							$this->validateColumnValue($tableModelForValidate, $column, $value[1]);
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
									$this->validateColumnValue($tableModelForValidate, $column, $v);
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
							$this->validateColumnValue($tableModelForValidate, $column, $value);

						$substr = $parsedColumn . ' ' . $operator . ' ' . $this->parseValue($value, $columnType);
						break;
				}

				$str[] = $substr;
			}
		}

		return implode(' ' . $options['glue'] . ' ', $str);
	}

	/**
	 * Possible formats for a join:
	 * - => 'table_name' (string)
	 * - 'table_name' => ['field1', 'field2']
	 * - 'table_name' => ['on' => 'this_field', 'fields' => ['field1', 'field2']]
	 * - 'table_name' => ['on' => ['this_field' => 'other_field'], 'fields' => ['field1', 'field2']]
	 * - 'table_name' => ['on' => 't1.field = t2.field', 'fields' => ['field1', 'field2']]
	 * - => ['table' => 'table_name', 'fields' => ['field1', 'field2']]
	 * - => ['table' => 'table_name', 'on' => ['this_field' => 'other_field'], 'fields' => ['field1', 'field2']]
	 * - => ['table' => 'table_name', 'on' => 't1.field = t2.field', 'fields' => ['field1', 'field2']]
	 *
	 * @param string $table
	 * @param array $joins
	 * @return array
	 * @throws \Exception
	 */
	private function normalizeJoins(string $table, array $joins): array
	{
		$normalized = [];

		foreach ($joins as $join_key => $join) {
			if (is_string($join))
				$join = ['table' => $join];
			if (!isset($join['table']) and !isset($join['fields']) and !isset($join['on']))
				$join = ['fields' => $join];
			if (!is_numeric($join_key) and !isset($join['table']))
				$join['table'] = $join_key;
			if (!isset($join['table']))
				throw new \Exception('Bad join format');

			if (isset($join['full-on']) or isset($join['full_on']))
				throw new \Exception('"full-on" option in joins is deprecated');
			if (isset($join['join-on']) or isset($join['join_field']))
				throw new \Exception('"join-on"/"join-field" options in joins are deprecated');
			if (isset($join['full_fields']))
				throw new \Exception('"full_fields" option in joins is deprecated');

			if (!isset($join['type']))
				$join['type'] = 'INNER';
			if (!isset($join['origin-table']))
				$join['origin-table'] = $table;
			if (!isset($join['fields']))
				$join['fields'] = [];

			$normalized[] = $join;
		}

		return $normalized;
	}

	/**
	 * @param array $joins
	 * @return string
	 * @throws \Exception
	 */
	private function buildJoins(array $joins): string
	{
		$join_str = [];

		foreach ($joins as $join) {
			$tableModel = $this->parser->getTable($join['origin-table']);
			$joinTableModel = $this->parser->getTable($join['table']);

			if (empty($join['on'])) {
				$fk_found = null;
				foreach ($tableModel->columns as $column) {
					foreach ($column['foreign_keys'] as $fk) {
						if ($fk['ref_table'] === $join['table']) {
							if ($fk_found === null)
								$fk_found = $fk;
							else // Ambiguous: two FKs for the same table
								throw new \Exception('Join error: two FKs found in table "' . $join['origin-table'] . '" towards table "' . $join['table'] . '".');
						}
					}
				}

				if ($fk_found !== null) {
					$join['on'] = [
						$fk_found['column'] => $fk_found['ref_column'],
					];
				} else {
					foreach ($joinTableModel->columns as $column) {
						foreach ($column['foreign_keys'] as $fk) {
							if ($fk['ref_table'] === $join['origin-table']) {
								if ($fk_found === null)
									$fk_found = $fk;
								else // Ambiguous: two FKs for the same table
									throw new \Exception('Join error: two FKs found in table "' . $join['table'] . '" towards table "' . $join['origin-table'] . '".');
							}
						}
					}

					if ($fk_found) {
						$join['on'] = [
							$fk_found['ref_column'] => $fk_found['column'],
						];
					}
				}

				if ($fk_found === null)
					throw new \Exception('Join error: no matching FK found between tables "' . $join['origin-table'] . '" e "' . $join['table'] . '".');
			} else if (is_string($join['on'])) {
				$join['on'] = [$join['on']];
			}

			$on_string = [];
			foreach ($join['on'] as $on_key => $on_value) {
				if (is_string($on_key) and is_string($on_value)) {
					$on_string[] = $this->parseColumn($on_key, ['table' => $join['origin-table']]) . '=' . $this->parseColumn($on_value, ['table' => $join['alias'] ?? $join['table']]);
				} elseif (is_string($on_value)) {
					if (str_contains($on_value, '=')) {
						// Is a full formed "on" clause
						$on_string[] = '(' . $on_value . ')';
					} else {
						// Is the name of a column, let's search a matching FK
						if (!isset($tableModel->columns[$on_value]))
							throw new \Exception('Join error: column "' . $on_value . '" does not exist in table "' . $join['origin-table'] . '"!');

						$fk_found = null;
						foreach ($tableModel->columns[$on_value]['foreign_keys'] as $foreign_key) {
							if ($foreign_key['ref_table'] === $join['table']) {
								$fk_found = $foreign_key['ref_column'];
								break;
							}
						}

						if (!$fk_found) {
							foreach ($joinTableModel->columns as $joinTableColumnName => $joinTableColumn) {
								foreach ($joinTableColumn['foreign_keys'] as $joinTableFk) {
									if ($joinTableFk['ref_table'] === $join['origin-table'] and $joinTableFk['ref_column'] === $on_value) {
										if ($fk_found === null)
											$fk_found = $joinTableColumnName;
										else
											throw new \Exception('Join error: two FKs found in table "' . $join['table'] . '" towards column "' . $on_value . '".');
									}
								}
							}
						}

						if (!$fk_found)
							throw new \Exception('Join error: no matching FK found for column "' . $on_value . '".');

						$on_string[] = $this->parseColumn($on_value, ['table' => $join['origin-table']]) . '=' . $this->parseColumn($fk_found, ['table' => $join['alias'] ?? $join['table']]);
					}
				} else {
					throw new \Exception('Bad join "on" format');
				}
			}

			$on_string = implode(' AND ', $on_string);

			if (!empty($join['where'])) {
				if (!is_string($join['where']))
					$join['where'] = $this->select($join['table'], $join['where'], ['alias' => $join['alias'] ?? null]);

				$on_string .= ' AND (' . $join['where'] . ')';
			}

			$join_str[] = ' ' . $join['type'] . ' JOIN `' . $join['table'] . '`' . (isset($join['alias']) ? ' AS `' . $join['alias'] . '`' : '') . ' ON ' . $on_string;
		}

		return implode('', $join_str);
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
				foreach ($join['fields'] as $fieldIdx => $field) {
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

		$column = $table->columns[$columnName];
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
			case 'time':
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
