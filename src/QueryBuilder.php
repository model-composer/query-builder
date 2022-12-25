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
	 * @return string|null
	 */
	public function insert(string $table, array $data = [], array $options = []): ?string
	{
		$options = array_merge([
			'replace' => false,
			'validate_data' => true,
		], $options);

		$keys = null;
		$qry_rows = [];

		$rows = $this->isAssoc($data) ? [$data] : $data;
		foreach ($rows as $row) {
			$rowKeys = [];

			if ($row === []) {
				$qry_rows[] = '()';
			} else {
				$values = [];
				foreach ($row as $k => $v) {
					[$realTable, $realColumn, $parsedColumn, $isFromJoin] = $this->parseInputColumn($k, $table);
					$rowKeys[] = $parsedColumn;

					if ($realTable) {
						$realTableModel = $this->parser->getTable($realTable);
						if (!isset($realTableModel->columns[$realColumn]))
							throw new \Exception('Column "' . $realColumn . '" does not exist in table "' . $realTable . '"');

						$columnType = $realTableModel->columns[$realColumn]['type'];
					} else {
						$realTableModel = null;
						$columnType = null;
					}

					if ($realTableModel and $options['validate_data'])
						$this->validateColumnValue($realTableModel, $realColumn, $v);

					$values[] = $this->parseValue($v, $columnType);
				}

				$qry_rows[] = '(' . implode(',', $values) . ')';
			}

			if ($keys === null)
				$keys = $rowKeys;
			elseif (json_encode($keys) !== json_encode($rowKeys))
				throw new \Exception('All rows must have identical fields in a bulk insert');
		}

		$qry = null;
		if (count($qry_rows) > 0)
			$qry = ($options['replace'] ? 'REPLACE' : 'INSERT') . ' INTO `' . $table . '`(' . implode(',', $keys) . ') VALUES' . implode(',', $qry_rows);

		return $qry;
	}

	/**
	 * Checks whether the given array is associative
	 *
	 * @param array $arr
	 * @return bool
	 */
	private function isAssoc(array $arr): bool
	{
		if ([] === $arr)
			return false;
		return array_keys($arr) !== range(0, count($arr) - 1);
	}

	/**
	 * @param string $table
	 * @param array|int $where
	 * @param array $data
	 * @param array $options
	 * @return string|null
	 * @throws \Exception
	 */
	public function update(string $table, array|int $where = [], array $data = [], array $options = []): ?string
	{
		if (empty($data))
			return null;

		$options = array_merge([
			'alias' => null,
			'joins' => [],
			'operator' => 'AND',
			'validate_where' => true,
			'validate_data' => true,
		], $options);

		$options['joins'] = $this->normalizeJoins($options['alias'] ?? $table, $options['joins']);

		$whereStr = $this->buildQueryString($where, [
			'table' => $table,
			'alias' => $options['alias'],
			'joins' => $options['joins'],
			'operator' => $options['operator'],
			'validate' => $options['validate_where'],
		]);

		$dataStr = $this->buildQueryString($data, [
			'table' => $table,
			'alias' => $options['alias'],
			'joins' => $options['joins'],
			'operator' => ',',
			'for-select' => false,
			'validate' => $options['validate_data'],
		]);

		$joinStr = $this->buildJoins($options['joins']);

		$qry = 'UPDATE `' . $table . '`' . ($options['alias'] ? ' AS `' . $options['alias'] . '`' : '') . $joinStr . ' SET ' . $dataStr;
		if ($whereStr)
			$qry .= ' WHERE ' . $whereStr;

		return $qry;
	}

	/**
	 * @param string $table
	 * @param array|int $where
	 * @param array $options
	 * @return string
	 * @throws \Exception
	 */
	public function delete(string $table, array|int $where = [], array $options = []): string
	{
		$options = array_merge([
			'alias' => null,
			'joins' => [],
			'operator' => 'AND',
			'validate_where' => true,
		], $options);

		$options['joins'] = $this->normalizeJoins($options['alias'] ?? $table, $options['joins']);

		$whereStr = $this->buildQueryString($where, [
			'table' => $table,
			'alias' => $options['alias'],
			'joins' => $options['joins'],
			'operator' => $options['operator'],
			'validate' => $options['validate_where'],
		]);

		$joinStr = $this->buildJoins($options['joins']);

		$qry = 'DELETE `' . ($options['alias'] ?? $table) . '` FROM `' . $table . '`' . ($options['alias'] ? ' AS `' . $options['alias'] . '`' : '') . $joinStr;
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
			'min' => [],
			'max' => [],
			'sum' => [],
			'avg' => [],
			'count' => [],
			'count_distinct' => [],
			'raw_fields' => false,
			'group_by' => null,
			'having' => [],
			'order_by' => null,
			'limit' => null,
			'offset' => null,
			'operator' => 'AND',
			'validate_where' => true,
		], $options);

		$options['joins'] = $this->normalizeJoins($options['alias'] ?? $table, $options['joins']);

		$whereStr = $this->buildQueryString($where, [
			'table' => $table,
			'alias' => $options['alias'],
			'joins' => $options['joins'],
			'operator' => $options['operator'],
			'validate' => $options['validate_where'],
		]);

		$joinStr = $this->buildJoins($options['joins']);

		$fields_str = [];
		if ($options['fields'] !== null) {
			if (is_array($options['fields'])) {
				if ($options['raw_fields']) {
					$fields_str = $options['fields'];
				} else {
					foreach ($options['fields'] as $k => $v) {
						$field = is_numeric($k) ? $v : $k;
						$alias = is_numeric($k) ? null : $v;

						[$realTable, $realColumn, $parsedColumn, $isFromJoin] = $this->parseInputColumn($v, $table, $options['joins'], $options['alias'] ?? null);
						$realTableModel = $this->parser->getTable($realTable);

						if (!isset($realTableModel->columns[$realColumn]))
							throw new \Exception('Field "' . $field . '" does not exist');

						if ($realTableModel->columns[$realColumn]['type'] === 'point')
							$fields_str[] = 'ST_AsText(' . $parsedColumn . ') AS ' . $this->parseColumn($alias ?? $field);
						else
							$fields_str[] = $parsedColumn . ($alias ? ' AS ' . $this->parseColumn($alias) : '');
					}
				}
			} elseif (is_string($options['fields'])) {
				$fields_str = [$options['fields']];
			} else {
				throw new \Exception('Error while building select query, "fields" must be either an array or a string');
			}
		} else {
			$tableModel = $this->parser->getTable($table);

			$fields_str[] = ($options['alias'] ?? $table) . '.*';
			foreach ($tableModel->columns as $field => $fieldOpt) {
				if ($fieldOpt['type'] === 'point') {
					$parsedField = $this->parseColumn($field, $options['alias'] ?? $table);
					$fields_str[] = 'ST_AsText(' . $parsedField . ') AS ' . $this->parseColumn($field);
				}
			}
		}

		$aggregations = [
			'min',
			'max',
			'sum',
			'avg',
			'count',
			'count_distinct',
		];

		foreach ($aggregations as $f) {
			if ($options[$f]) {
				if (!is_array($options[$f]))
					$options[$f] = [$options[$f]];

				foreach ($options[$f] as $k => $v) {
					$field = is_numeric($k) ? $v : $k;

					if (str_contains($field, '.')) {
						$field = explode('.', $field);
						$referenceTable = $field[0];
						$field = $field[1];
						$alias = is_numeric($k) ? $field : $v;
					} else {
						$referenceTable = $options['alias'] ?? $table;
						$alias = $v;
					}

					$str_beginning = $f === 'count_distinct' ? 'COUNT(DISTINCT ' : strtoupper($f) . '(';
					$fields_str[] = $str_beginning . $this->parseColumn($field, $referenceTable) . ') AS ' . $alias;
				}
			}
		}

		$fields_str = implode(',', $fields_str);

		if ($options['fields'] === null) {
			$fields_from_joins = [];
			foreach ($options['joins'] as $join) {
				$joinTableModel = $this->parser->getTable($join['table']);
				foreach ($join['fields'] as $fieldIdx => $field) {
					$tableName = $join['alias'] ?? $join['table'];

					$realColumn = is_numeric($fieldIdx) ? $field : $fieldIdx;
					$alias = $field;

					if ($joinTableModel->columns[$realColumn]['type'] === 'point') {
						$this_field_str = 'ST_AsText(' . $this->parseColumn($realColumn, $tableName) . ') AS ' . $this->parseColumn($alias);
					} else {
						$this_field_str = $this->parseColumn($realColumn, $tableName);
						if ($alias !== $realColumn)
							$this_field_str .= ' AS ' . $this->parseColumn($alias);
					}

					$fields_from_joins[] = $this_field_str;
				}
			}

			if ($fields_from_joins) {
				if (str_starts_with($fields_str, '*'))
					$fields_str = '`' . $table . '`.' . $fields_str;
				$fields_str .= ',' . implode(',', $fields_from_joins);
			}
		}

		$qry = 'SELECT ' . $fields_str . ' FROM `' . $table . '`' . $joinStr;
		if ($whereStr)
			$qry .= ' WHERE ' . $whereStr;

		if ($options['group_by']) {
			if (!is_array($options['group_by']))
				$options['group_by'] = [$options['group_by']];

			foreach ($options['group_by'] as &$field)
				[$_1, $_2, $field, $_3] = $this->parseInputColumn($field, $table, $options['joins'], $options['alias'] ?? null);
			unset($field);

			$qry .= ' GROUP BY ' . implode(',', $options['group_by']);
			if ($options['having'])
				$qry .= ' HAVING ' . $this->buildQueryString($options['having'], ['validate' => false]);
		}

		if ($options['order_by']) {
			if (is_array($options['order_by'])) {
				foreach ($options['order_by'] as &$sortingField) {
					if (!is_array($sortingField))
						$sortingField = [$sortingField, 'ASC'];
					if (!in_array(strtoupper($sortingField[1]), ['ASC', 'DESC']))
						throw new \Exception('Bad "order by" direction');

					[$_1, $_2, $sortingField[0], $_3] = $this->parseInputColumn($sortingField[0], $table, $options['joins'], $options['alias'] ?? null);
					$sortingField = implode(' ', $sortingField);
				}
				unset($sortingField);

				$qry .= ' ORDER BY ' . implode(',', $options['order_by']);
			} else {
				$qry .= ' ORDER BY ' . $options['order_by'];
			}
		}

		if ($options['limit'] !== null) {
			if (!is_numeric($options['limit']))
				throw new \Exception('Non-numeric limit is deprecated');

			$limitQry = $options['limit'];
			if ($options['offset'] !== null) {
				if (!is_numeric($options['offset']))
					throw new \Exception('Offset must be numeric');

				$limitQry = $options['offset'] . ',' . $limitQry;
			}

			$qry .= ' LIMIT ' . $limitQry;
		} elseif ($options['offset']) {
			throw new \Exception('Offset option must have a limit set as well');
		}

		return $qry;
	}

	/**
	 * @param array $queries
	 * @param array $options
	 * @return string|null
	 */
	public function unionSelect(array $queries, array $options = []): ?string
	{
		$qry_str = [];
		foreach ($queries as $qryOptions) {
			$copiedQueryOptions = $options;
			if (isset($copiedQueryOptions['order_by']))
				unset($copiedQueryOptions['order_by']);
			if (isset($copiedQueryOptions['limit']))
				unset($copiedQueryOptions['limit']);

			$singleQueryOptions = $qryOptions['options'] ?? [];
			$copiedQueryOptions = $this->array_merge_recursive_distinct($singleQueryOptions, $copiedQueryOptions);
			$qry_str[] = $this->select($qryOptions['table'], $qryOptions['where'] ?? [], $copiedQueryOptions);
		}

		if (empty($qry_str))
			return null;

		$qry = implode(' UNION ', $qry_str);

		if (($options['order_by'] ?? null) !== null) {
			if (is_array($options['order_by'])) {
				foreach ($options['order_by'] as &$sortingField) {
					if (!is_array($sortingField))
						$sortingField = [$sortingField, 'ASC'];
					if (!in_array(strtoupper($sortingField[1]), ['ASC', 'DESC']))
						throw new \Exception('Bad "order by" direction');

					$sortingField[0] = $this->parseColumn($sortingField[0]);
					$sortingField = implode(' ', $sortingField);
				}
				unset($sortingField);

				$qry .= ' ORDER BY ' . implode(',', $options['order_by']);
			} else {
				$qry .= ' ORDER BY ' . $options['order_by'];
			}
		}

		if (($options['limit'] ?? null) !== null)
			$qry .= ' LIMIT ' . $options['limit'];

		return $qry;
	}

	/**
	 * Utility for the previous method
	 *
	 * @param array $array1
	 * @param array $array2
	 * @return array
	 */
	private function array_merge_recursive_distinct(array &$array1, array &$array2): array
	{
		$merged = $array1;

		foreach ($array2 as $key => &$value) {
			if (is_numeric($key))
				$merged[] = $value;
			elseif (is_array($value) && isset ($merged [$key]) && is_array($merged [$key]))
				$merged[$key] = $this->array_merge_recursive_distinct($merged [$key], $value);
			else
				$merged[$key] = $value;
		}

		return $merged;
	}

	/**
	 * @param array|int $where
	 * @param array $options
	 * @return string
	 */
	public function buildQueryString(array|int $where, array $options = []): string
	{
		if (!is_array($where))
			$where = ['id' => $where];

		$options = array_merge([
			'table' => null,
			'alias' => null,
			'operator' => 'AND',
			'for-select' => true,
			'joins' => [],
			'validate' => true,
		], $options);

		$tableModel = null;
		if ($options['table'])
			$tableModel = $this->parser->getTable($options['table']);

		$str = [];

		foreach ($where as $k => $item) {
			$substr = null;

			if (is_array($item)) {
				if (in_array(strtoupper($k), ['OR', 'AND'])) {
					$substr = $this->buildQueryString($item, array_merge($options, ['operator' => strtoupper($k)]));
				} else {
					switch (count($item)) {
						case 1:
							if (in_array(strtoupper(array_key_first($item)), ['OR', 'AND']))
								$substr = $this->buildQueryString(reset($item), array_merge($options, ['operator' => strtoupper(array_key_first($item))]));
							else
								throw new \Exception('Query build error: wrong items number in array #1');
							break;
						case 2:
							if (is_numeric($k)) {
								if (isset($item[0]) and in_array(strtoupper($item[0]), ['OR', 'AND'])) {
									if (!is_array($item[1]))
										throw new \Exception('Operator "' . $item[0] . '" needs an array');

									$substr = $this->buildQueryString($item[1], array_merge($options, ['operator' => strtoupper($item[0])]));
								} elseif (isset($item['sub'], $item['operator'])) {
									if (!in_array(strtoupper($item['operator']), ['OR', 'AND']))
										throw new \Exception('Operator "' . $item['operator'] . '" not supported');

									$substr = $this->buildQueryString($item['sub'], array_merge($options, ['operator' => strtoupper($item['operator'])]));
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

				if ($operator === 'MATCH') {
					if (!is_array($column)) {
						if (is_string($column))
							$column = [$column];
						else
							throw new \Exception('Column name must be a string or an array, with MATCH operator');
					}

					$parsedColumnArr = [];
					foreach ($column as $c) {
						[$_1, $_2, $parsedColumn, $_3] = $this->parseInputColumn($c, $options['table'], $options['joins'], $options['alias'] ?? null);
						$parsedColumnArr[] = $parsedColumn;
					}

					$substr = 'MATCH(' . implode(',', $parsedColumnArr) . ') AGAINST(' . $this->parseValue($value) . ')';
				} else {
					if (!is_string($column))
						throw new \Exception('Column name must be a string');

					[$realTable, $realColumn, $parsedColumn, $isFromJoin] = $this->parseInputColumn($column, $options['table'], $options['joins'], $options['alias'] ?? null);
					if ($realTable) {
						$realTableModel = $this->parser->getTable($realTable);
						if (!isset($realTableModel->columns[$realColumn]))
							throw new \Exception('Column "' . $realColumn . '" does not exist in table "' . $realTable . '"');

						$columnType = $realTableModel->columns[$realColumn]['type'];
					} else {
						$realTableModel = null;
						$columnType = null;
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

							if ($value[0] === null or $value[1] === null)
								throw new \Exception('"between" cannot accept null values');

							if ($realTableModel and $options['validate']) {
								$this->validateColumnValue($realTableModel, $realColumn, $value[0]);
								$this->validateColumnValue($realTableModel, $realColumn, $value[1]);
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
									if ($realTableModel and $options['validate'] and ($v !== null or !$isFromJoin))
										$this->validateColumnValue($realTableModel, $realColumn, $v);
									$parsedValues[] = $this->parseValue($v, $columnType);
								}

								$substr = $parsedColumn . ' ' . $operator . ' (' . implode(',', $parsedValues) . ')';
							}
							break;
						default:
							if ($realTableModel and $options['validate'] and ($value !== null or !$isFromJoin))
								$this->validateColumnValue($realTableModel, $realColumn, $value);

							$substr = $parsedColumn . ' ' . $operator . ' ' . $this->parseValue($value, $columnType);
							break;
					}
				}

				$str[] = $substr;
			}
		}

		return implode(' ' . $options['operator'] . ' ', $str);
	}

	/**
	 * @param array $where
	 * @return array
	 * @throws \Exception
	 */
	public function getFieldsInvolvedInWhere(array $where): array
	{
		$fields = [];

		foreach ($where as $k => $item) {
			if (is_array($item)) {
				if (in_array(strtoupper($k), ['OR', 'AND'])) {
					foreach ($this->getFieldsInvolvedInWhere($item) as $f)
						$fields[] = $f;
				} else {
					switch (count($item)) {
						case 1:
							if (in_array(strtoupper(array_key_first($item)), ['OR', 'AND'])) {
								foreach ($this->getFieldsInvolvedInWhere(reset($item)) as $f)
									$fields[] = $f;
							} else {
								throw new \Exception('Query build error: wrong items number in array #1');
							}
							break;

						case 2:
							if (is_numeric($k)) {
								if (isset($item[0]) and in_array(strtoupper($item[0]), ['OR', 'AND'])) {
									if (!is_array($item[1]))
										throw new \Exception('Operator "' . $item[0] . '" needs an array');

									foreach ($this->getFieldsInvolvedInWhere($item[1]) as $f)
										$fields[] = $f;
								} elseif (isset($item['sub'], $item['operator'])) {
									foreach ($this->getFieldsInvolvedInWhere($item['sub']) as $f)
										$fields[] = $f;
								} else {
									$fields[] = $item[0];
								}
							} else {
								$fields[] = $k;
							}
							break;

						case 3:
							$fields[] = $item[0];
							break;

						default:
							throw new \Exception('Query build error: wrong items number in array #2');
					}
				}
			} else {
				if (!is_numeric($k))
					$fields[] = $k;
			}
		}

		return $fields;
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
	public function normalizeJoins(string $table, array $joins): array
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
					$on_string[] = $this->parseColumn($on_key, $join['origin-table']) . '=' . $this->parseColumn($on_value, $join['alias'] ?? $join['table']);
				} elseif (is_string($on_value)) {
					if (str_contains($on_value, '=') or str_contains($on_value, ' LIKE ')) {
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

						$on_string[] = $this->parseColumn($on_value, $join['origin-table']) . '=' . $this->parseColumn($fk_found, $join['alias'] ?? $join['table']);
					}
				} else {
					throw new \Exception('Bad join "on" format');
				}
			}

			$on_string = implode(' AND ', $on_string);

			if (!empty($join['where'])) {
				if (!is_string($join['where'])) {
					$join['where'] = $this->buildQueryString($join['where'], [
						'table' => $join['table'],
						'alias' => $join['alias'] ?? null,
					]);
				}

				$on_string .= ' AND (' . $join['where'] . ')';
			}

			$join_str[] = ' ' . $join['type'] . ' JOIN `' . $join['table'] . '`' . (isset($join['alias']) ? ' AS `' . $join['alias'] . '`' : '') . ' ON ' . $on_string;
		}

		return implode('', $join_str);
	}

	/**
	 * @param string $column
	 * @param string $table
	 * @param array $joins
	 * @param string|null $alias
	 * @return array
	 */
	public function parseInputColumn(string $column, ?string $table = null, array $joins = [], ?string $alias = null): array
	{
		if (str_contains($column, '.')) {
			$column = explode('.', $column);
			if (count($column) !== 2)
				throw new \Exception('Wrong column name format');

			$parsed = $this->parseColumn($column[1], $column[0]);

			return [
				null,
				$column[1],
				$parsed,
				false,
			];
		} else {
			$alias ??= $table;
			$isFromJoin = false;

			foreach ($joins as $joinIdx => $join) {
				foreach (($join['fields'] ?? []) as $fieldIdx => $field) {
					$fieldName = is_numeric($fieldIdx) ? $field : $fieldIdx;

					if ($field === $column) {
						$isFromJoin = true;
						$table = $join['table'] ?? $joinIdx;
						$alias = $join['alias'] ?? $table;
						$column = $fieldName;
						break 2;
					}
				}
			}

			$parsed = $this->parseColumn($column, $alias);

			return [
				$table,
				$column,
				$parsed,
				$isFromJoin,
			];
		}
	}

	/**
	 * @param string $column
	 * @param string|null $table
	 * @return string
	 */
	public function parseColumn(string $column, ?string $table = null): string
	{
		$column = preg_replace('/[^a-zA-Z0-9_.,()!=<> -]+/', '', $column);
		if (str_contains($column, '.')) {
			$column = explode('.', $column);
			if (count($column) !== 2)
				throw new \Exception('Wrong column name format');

			return '`' . $column[0] . '`.`' . $column[1] . '`';
		} else {
			return $table ? '`' . $table . '`.`' . $column . '`' : '`' . $column . '`';
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
