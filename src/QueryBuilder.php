<?php

namespace yii\couchdb;

use yii\base\InvalidParamException;
use yii\base\BaseObject;
use yii\helpers\ArrayHelper;

/**
 * QueryBuilder builds a CouchDB command statements.
 * It is used by [[Command]] for particular commands and queries composition.
 *
 * CouchDB uses JSON format to specify query conditions with quite specific syntax.
 * However [[buildCondition()]] method provides the ability of "translating" common condition format used "yii\db\*"
 * into CouchDB condition.
 * For example:
 *
 * ```php
 * $condition = [
 *     [
 *         'OR',
 *         ['AND', ['first_name' => 'John'], ['last_name' => 'Smith']],
 *         ['status' => [1, 2, 3]]
 *     ],
 * ];
 * print_r(Yii::$app->couchdb->getQueryBuilder()->buildCondition($condition));
 * // outputs :
 * [
 *     '$or' => [
 *         [
 *             'first_name' => 'John',
 *             'last_name' => 'John',
 *         ],
 *         [
 *             'status' => ['$in' => [1, 2, 3]],
 *         ]
 *     ]
 * ]
 * ```
 *
 * Note: condition values for the key '_id' will be automatically cast to [[JSON]] instance,
 * @todo containing???
 * even if they are plain strings. However, if you have other columns, containing [[???]], you
 * should take care of possible typecast on your own.
 *
 * @author Dan Orton <dan.orton84@gmail.com>
 */
class QueryBuilder extends BaseObject
{
    /**
     * @var Connection the CouchDB connection.
     */
    public $db;


    /**
     * Constructor.
     * @param Connection $connection the database connection.
     * @param array $config name-value pairs that will be used to initialize the object properties
     */
    public function __construct($connection, $config = [])
    {
        $this->db = $connection;
        parent::__construct($config);
    }

    // Commands :

    /**
     * Generates 'create collection' command.
     * @param string $collectionName collection name.
     * @param array $options collection options in format: "name" => "value"
     * @return array command document.
     */
    public function createCollection($collectionName, array $options = [])
    {
        $document = array_merge(['create' => $collectionName], $options);

        if (isset($document['indexOptionDefaults'])) {
            $document['indexOptionDefaults'] = (object) $document['indexOptionDefaults'];
        }
        if (isset($document['storageEngine'])) {
            $document['storageEngine'] = (object) $document['storageEngine'];
        }
        if (isset($document['validator'])) {
            $document['validator'] = (object) $document['validator'];
        }

        return $document;
    }

    /**
     * Generates drop database command.
     * @return array command document.
     */
    public function dropDatabase()
    {
        return ['dropDatabase' => 1];
    }

    /**
     * Generates drop collection command.
     * @param string $collectionName name of the collection to be dropped.
     * @return array command document.
     */
    public function dropCollection($collectionName)
    {
        return ['drop' => $collectionName];
    }

    /**
     * Generates create indexes command.
     * @param string|null $databaseName database name.
     * @param string $collectionName collection name.
     * @param array[] $indexes indexes specification. Each specification should be an array in format: optionName => value
     *
     * @return array command document.
     */
    public function createIndexes($databaseName, $collectionName, $indexes)
    {
        $normalizedIndexes = [];

        foreach ($indexes as $index) {
            if (!isset($index['key'])) {
                throw new InvalidParamException('"key" is required for index specification');
            }

            $index['key'] = $this->buildSortFields($index['key']);

            if (!isset($index['ns'])) {
                if ($databaseName === null) {
                    $databaseName = $this->db->getDefaultDatabaseName();
                }
                $index['ns'] = $databaseName . '.' . $collectionName;
            }

            if (!isset($index['name'])) {
                $index['name'] = $this->generateIndexName($index['key']);
            }

            $normalizedIndexes[] = $index;
        }

        return [
            'createIndexes' => $collectionName,
            'indexes' => $normalizedIndexes,
        ];
    }

    /**
     * Generates index name for the given column orders.
     * Columns should be normalized using [[buildSortFields()]] before being passed to this method.
     * @param array $columns columns with sort order.
     * @return string index name.
     */
    public function generateIndexName($columns)
    {
        $parts = [];
        foreach ($columns as $column => $order) {
            $parts[] = $column . '_' . $order;
        }
        return implode('_', $parts);
    }

    /**
     * Generates drop indexes command.
     * @param string $collectionName collection name
     * @param string $index index name or pattern, use `*` in order to drop all indexes.
     * @return array command document.
     */
    public function dropIndexes($collectionName, $index)
    {
        return [
            'dropIndexes' => $collectionName,
            'index' => $index,
        ];
    }

    /**
     * Generates list indexes command.
     * @param string $collectionName collection name
     * @param array $options command options.
     * Available options are:
     *
     * - maxTimeMS: int, max execution time in ms.
     *
     * @return array command document.
     */
    public function listIndexes($collectionName, $options = [])
    {
        return array_merge(['listIndexes' => $collectionName], $options);
    }

    /**
     * Generates count command
     * @param string $collectionName
     * @param array $condition
     * @param array $options
     * @return array command document.
     */
    public function count($collectionName, $condition = [], $options = [])
    {
        $document = ['count' => $collectionName];

        if (!empty($condition)) {
            $document['query'] = (object) $this->buildCondition($condition);
        }

        return array_merge($document, $options);
    }

    /**
     * Generates 'map-reduce' command.
     * @see https://pouchdb.com/api.html#query_database
     * @param string $collectionName collection name.
     * @param JSON|string $map function, which emits map data from collection.
     * Argument will be automatically cast to [[JSON]].
     * @param JSON|string $reduce function that takes two arguments (the map key
     * and the map values) and does the aggregation.
     * Argument will be automatically cast to [[JSON]].
     * @param string|array $out output collection name. It could be a string for simple output
     * ('outputCollection'), or an array for parametrized output (['merge' => 'outputCollection']).
     * You can pass ['inline' => true] to fetch the result at once without temporary collection usage.
     * @param array $condition filter condition for including a document in the aggregation.
     * @param array $options additional optional parameters to the mapReduce command. 
     *
     * @return array command document.
     */
    public function mapReduce($collectionName, $map, $reduce, $out, $condition = [], $options = [])
    {
        if (!($map instanceof Javascript)) {
            $map = new Javascript((string) $map);
        }
        if (!($reduce instanceof Javascript)) {
            $reduce = new Javascript((string) $reduce);
        }

        $document = [
            'mapReduce' => $collectionName,
            'map' => $map,
            'reduce' => $reduce,
            'out' => $out
        ];

        if (!empty($condition)) {
            $document['query'] = $this->buildCondition($condition);
        }

        if (!empty($options)) {
            $document = array_merge($document, $options);
        }

        return $document;
    }

    

    /**
     * Generates 'explain' command.
     * @param string $collectionName collection name.
     * @param array $query query options.
     * @return array command document.
     */
    public function explain($collectionName, $query)
    {
        $query = array_merge(
            ['find' => $collectionName],
            $query
        );

        if (isset($query['filter'])) {
            $query['filter'] = (object) $this->buildCondition($query['filter']);
        }
        if (isset($query['projection'])) {
            $query['projection'] = $this->buildSelectFields($query['projection']);
        }
        if (isset($query['sort'])) {
            $query['sort'] = $this->buildSortFields($query['sort']);
        }

        return [
            'explain' => $query,
        ];
    }

    /**
     * Generates 'listDatabases' command.
     * @param array $condition filter condition.
     * @param array $options command options.
     * @return array command document.
     */
    public function listDatabases($condition = [], $options = [])
    {
        $document = array_merge(['listDatabases' => 1], $options);
        if (!empty($condition)) {
            $document['filter'] = (object)$this->buildCondition($condition);
        }
        return $document;
    }

    /**
     * Generates 'listCollections' command.
     * @param array $condition filter condition.
     * @param array $options command options.
     * @return array command document.
     */
    public function listCollections($condition = [], $options = [])
    {
        $document = array_merge(['listCollections' => 1], $options);
        if (!empty($condition)) {
            $document['filter'] = (object)$this->buildCondition($condition);
        }
        return $document;
    }

    // Service :

    /**
     * Normalizes fields list for the CouchDB select composition.
     * @param array|string $fields raw fields.
     * @return array normalized select fields.
     */
    public function buildSelectFields($fields)
    {
        $selectFields = [];
        foreach ((array)$fields as $key => $value) {
            if (is_int($key)) {
                $selectFields[$value] = true;
            } else {
                $selectFields[$key] = is_scalar($value) ? (bool)$value : $value;
            }
        }
        return $selectFields;
    }

    /**
     * Normalizes fields list for the CouchDB sort composition.
     * @param array|string $fields raw fields.
     * @return array normalized sort fields.
     */
    public function buildSortFields($fields)
    {
        $sortFields = [];
        foreach ((array)$fields as $key => $value) {
            if (is_int($key)) {
                $sortFields[$value] = +1;
            } else {
                if ($value === SORT_ASC) {
                    $value = +1;
                } elseif ($value === SORT_DESC) {
                    $value = -1;
                }
                $sortFields[$key] = $value;
            }
        }
        return $sortFields;
    }

    /**
     * Converts "\yii\db\*" quick condition keyword into actual CouchDB condition keyword.
     * @param string $key raw condition key.
     * @return string actual key.
     */
    protected function normalizeConditionKeyword($key)
    {
        static $map = [
            'AND' => '$and',
            'OR' => '$or',
            'IN' => '$in',
            'NOT IN' => '$nin',
        ];
        $matchKey = strtoupper($key);
        if (array_key_exists($matchKey, $map)) {
            return $map[$matchKey];
        }
        return $key;
    }

    /**
     * Converts given value into [[ObjectID]] instance.
     * If array given, each element of it will be processed.
     * @param mixed $rawId raw id(s).
     * @return array|ObjectID normalized id(s).
     */
    protected function ensureCouchDBId($rawId)
    {
        if (is_array($rawId)) {
            $result = [];
            foreach ($rawId as $key => $value) {
                $result[$key] = $this->ensureCouchDBId($value);
            }

            return $result;
        } elseif (is_object($rawId)) {
            if ($rawId instanceof ObjectID) {
                return $rawId;
            } else {
                $rawId = (string) $rawId;
            }
        }
        try {
            $CouchDBId = new ObjectID($rawId);
        } catch (InvalidArgumentException $e) {
            // invalid id format
            $CouchDBId = $rawId;
        }

        return $CouchDBId;
    }

    /**
     * Parses the condition specification and generates the corresponding CouchDB condition.
     * @param array $condition the condition specification. Please refer to [[Query::where()]]
     * on how to specify a condition.
     * @return array the generated CouchDB condition
     * @throws InvalidParamException if the condition is in bad format
     */
    public function buildCondition($condition)
    {
        static $builders = [
            'NOT' => 'buildNotCondition',
            'AND' => 'buildAndCondition',
            'OR' => 'buildOrCondition',
            'BETWEEN' => 'buildBetweenCondition',
            'NOT BETWEEN' => 'buildBetweenCondition',
            'IN' => 'buildInCondition',
            'NOT IN' => 'buildInCondition',
            'REGEX' => 'buildRegexCondition',
            'LIKE' => 'buildLikeCondition',
        ];

        if (!is_array($condition)) {
            throw new InvalidParamException('Condition should be an array.');
        } elseif (empty($condition)) {
            return [];
        }
        if (isset($condition[0])) { // operator format: operator, operand 1, operand 2, ...
            $operator = strtoupper($condition[0]);
            if (isset($builders[$operator])) {
                $method = $builders[$operator];
            } else {
                $operator = $condition[0];
                $method = 'buildSimpleCondition';
            }
            array_shift($condition);
            return $this->$method($operator, $condition);
        }
        // hash format: 'column1' => 'value1', 'column2' => 'value2', ...
        return $this->buildHashCondition($condition);
    }

    /**
     * Creates a condition based on column-value pairs.
     * @param array $condition the condition specification.
     * @return array the generated CouchDB condition.
     */
    public function buildHashCondition($condition)
    {
        $result = [];
        foreach ($condition as $name => $value) {
            if (strncmp('$', $name, 1) === 0) {
                // Native CouchDB condition:
                $result[$name] = $value;
            } else {
                if (is_array($value)) {
                    if (ArrayHelper::isIndexed($value)) {
                        // Quick IN condition:
                        $result = array_merge($result, $this->buildInCondition('IN', [$name, $value]));
                    } else {
                        // CouchDB complex condition:
                        $result[$name] = $value;
                    }
                } else {
                    // Direct match:
                    if ($name == '_id') {
                        $value = $this->ensureCouchDBId($value);
                    }
                    $result[$name] = $value;
                }
            }
        }

        return $result;
    }

    /**
     * Composes `NOT` condition.
     * @param string $operator the operator to use for connecting the given operands
     * @param array $operands the CouchDB conditions to connect.
     * @return array the generated CouchDB condition.
     * @throws InvalidParamException if wrong number of operands have been given.
     */
    public function buildNotCondition($operator, $operands)
    {
        if (count($operands) !== 2) {
            throw new InvalidParamException("Operator '$operator' requires two operands.");
        }

        list($name, $value) = $operands;

        $result = [];
        if (is_array($value)) {
            $result[$name] = ['$not' => $this->buildCondition($value)];
        } else {
            if ($name == '_id') {
                $value = $this->ensureCouchDBId($value);
            }
            $result[$name] = ['$ne' => $value];
        }

        return $result;
    }

    /**
     * Connects two or more conditions with the `AND` operator.
     * @param string $operator the operator to use for connecting the given operands
     * @param array $operands the CouchDB conditions to connect.
     * @return array the generated CouchDB condition.
     */
    public function buildAndCondition($operator, $operands)
    {
        $operator = $this->normalizeConditionKeyword($operator);
        $parts = [];
        foreach ($operands as $operand) {
            $parts[] = $this->buildCondition($operand);
        }

        return [$operator => $parts];
    }

    /**
     * Connects two or more conditions with the `OR` operator.
     * @param string $operator the operator to use for connecting the given operands
     * @param array $operands the CouchDB conditions to connect.
     * @return array the generated CouchDB condition.
     */
    public function buildOrCondition($operator, $operands)
    {
        $operator = $this->normalizeConditionKeyword($operator);
        $parts = [];
        foreach ($operands as $operand) {
            $parts[] = $this->buildCondition($operand);
        }

        return [$operator => $parts];
    }

    /**
     * Creates an CouchDB condition, which emulates the `BETWEEN` operator.
     * @param string $operator the operator to use
     * @param array $operands the first operand is the column name. The second and third operands
     * describe the interval that column value should be in.
     * @return array the generated CouchDB condition.
     * @throws InvalidParamException if wrong number of operands have been given.
     */
    public function buildBetweenCondition($operator, $operands)
    {
        if (!isset($operands[0], $operands[1], $operands[2])) {
            throw new InvalidParamException("Operator '$operator' requires three operands.");
        }
        list($column, $value1, $value2) = $operands;

        if (strncmp('NOT', $operator, 3) === 0) {
            return [
                $column => [
                    '$lt' => $value1,
                    '$gt' => $value2,
                ]
            ];
        }
        return [
            $column => [
                '$gte' => $value1,
                '$lte' => $value2,
            ]
        ];
    }

    /**
     * Creates an CouchDB condition with the `IN` operator.
     * @param string $operator the operator to use (e.g. `IN` or `NOT IN`)
     * @param array $operands the first operand is the column name. If it is an array
     * a composite IN condition will be generated.
     * The second operand is an array of values that column value should be among.
     * @return array the generated CouchDB condition.
     * @throws InvalidParamException if wrong number of operands have been given.
     */
    public function buildInCondition($operator, $operands)
    {
        if (!isset($operands[0], $operands[1])) {
            throw new InvalidParamException("Operator '$operator' requires two operands.");
        }

        list($column, $values) = $operands;

        $values = (array) $values;
        $operator = $this->normalizeConditionKeyword($operator);

        if (!is_array($column)) {
            $columns = [$column];
            $values = [$column => $values];
        } elseif (count($column) > 1) {
            return $this->buildCompositeInCondition($operator, $column, $values);
        } else {
            $columns = $column;
            $values = [$column[0] => $values];
        }

        $result = [];
        foreach ($columns as $column) {
            if ($column == '_id') {
                $inValues = $this->ensureCouchDBId($values[$column]);
            } else {
                $inValues = $values[$column];
            }

            $inValues = array_values($inValues);
            if (count($inValues) === 1 && $operator === '$in') {
                $result[$column] = $inValues[0];
            } else {
                $result[$column][$operator] = $inValues;
            }
        }

        return $result;
    }

    /**
     * @param string $operator CouchDB the operator to use (`$in` OR `$nin`)
     * @param array $columns list of compare columns
     * @param array $values compare values in format: columnName => [values]
     * @return array the generated CouchDB condition.
     */
    private function buildCompositeInCondition($operator, $columns, $values)
    {
        $result = [];

        $inValues = [];
        foreach ($values as $columnValues) {
            foreach ($columnValues as $column => $value) {
                if ($column == '_id') {
                    $value = $this->ensureCouchDBId($value);
                }
                $inValues[$column][] = $value;
            }
        }

        foreach ($columns as $column) {
            $columnInValues = array_values($inValues[$column]);
            if (count($columnInValues) === 1 && $operator === '$in') {
                $result[$column] = $columnInValues[0];
            } else {
                $result[$column][$operator] = $columnInValues;
            }
        }

        return $result;
    }

    /**
     * Creates a CouchDB regular expression condition.
     * @param string $operator the operator to use
     * @param array $operands the first operand is the column name.
     * The second operand is a single value that column value should be compared with.
     * @return array the generated CouchDB condition.
     * @throws InvalidParamException if wrong number of operands have been given.
     */
    public function buildRegexCondition($operator, $operands)
    {
        if (!isset($operands[0], $operands[1])) {
            throw new InvalidParamException("Operator '$operator' requires two operands.");
        }
        list($column, $value) = $operands;
        if (!($value instanceof Regex)) {
            if (preg_match('~\/(.+)\/(.*)~', $value, $matches)) {
                $value = new Regex($matches[1], $matches[2]);
            } else {
                $value = new Regex($value, '');
            }
        }

        return [$column => $value];
    }

    /**
     * Creates a CouchDB condition, which emulates the `LIKE` operator.
     * @param string $operator the operator to use
     * @param array $operands the first operand is the column name.
     * The second operand is a single value that column value should be compared with.
     * @return array the generated CouchDB condition.
     * @throws InvalidParamException if wrong number of operands have been given.
     */
    public function buildLikeCondition($operator, $operands)
    {
        if (!isset($operands[0], $operands[1])) {
            throw new InvalidParamException("Operator '$operator' requires two operands.");
        }
        list($column, $value) = $operands;
        if (!($value instanceof Regex)) {
            $value = new Regex(preg_quote($value), 'i');
        }

        return [$column => $value];
    }

    /**
     * Creates an CouchDB condition like `{$operator:{field:value}}`.
     * @param string $operator the operator to use. Besides regular CouchDB operators, aliases like `>`, `<=`,
     * and so on, can be used here.
     * @param array $operands the first operand is the column name.
     * The second operand is a single value that column value should be compared with.
     * @return string the generated CouchDB condition.
     * @throws InvalidParamException if wrong number of operands have been given.
     */
    public function buildSimpleCondition($operator, $operands)
    {
        if (count($operands) !== 2) {
            throw new InvalidParamException("Operator '$operator' requires two operands.");
        }

        list($column, $value) = $operands;

        if (strncmp('$', $operator, 1) !== 0) {
            static $operatorMap = [
                '>' => '$gt',
                '<' => '$lt',
                '>=' => '$gte',
                '<=' => '$lte',
                '!=' => '$ne',
                '<>' => '$ne',
                '=' => '$eq',
                '==' => '$eq',
            ];
            if (isset($operatorMap[$operator])) {
                $operator = $operatorMap[$operator];
            } else {
                throw new InvalidParamException("Unsupported operator '{$operator}'");
            }
        }

        return [$column => [$operator => $value]];
    }
}