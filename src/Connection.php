<?php

namespace yii\couchdb;

use GuzzleHttp\Client;
use yii\base\Component;
use yii\base\InvalidConfigException;
use Yii;

/**
 * Connection represents a connection to CouchDB server.
 * 
 * Connection works together with [[Database]] and [[Collection]] to provide data access
 * to the Couch database. They provide the connection using the [[GuzzleHttp extension]].
 * 
 * To establish a connection, set [[dsn]] and then call [[open()]] to be true.
 * 
 * The following example shows how to create a Connection instance and establish
 * the DB connection:
 * 
 * ```php
 * $connection = new \yii\couchdb\Connection([
 *      'dsn' => $dsn,
 * ]);
 * $connection -> open();
 * ```
 * 
 * After the CouchDB connection is established, one can access CouchDB databases and collections.
 * 
 * ```php
 * $database = $connection->getDatabase('my_couchdb_db');
 * $collection = $database->getCollection('customer');
 * $collection->insert(['name' => 'John Smith', 'status' => 1]);
 * ```
 * 
 * You can work with several different databases at the same server using this class.
 * However, while it is unlikely your application will need it, the Connection class
 * provides ability to use [[defaultDatabaseName]] as well as a shortcut method [[getCollection()]]
 * to retrieve a particular collection instance:
 * ```php
 * //get collection from default database:
 * $collection = $connection->getCollection('customer');
 * //get collection 'customer' from database 'myDatabase':
 * $collection = $connection->getCollection(['myDatabase', 'customer']);
 * ```
 * 
 * Connection is often used as an application component and configured in the application
 * configuration like the following:
 * 
 * ```php
 * [
 *      'components' => [
 *          'couchdb' => [
 *              'class' => '\yii\couchdb\Connection',
 *              'dsn' => 'http://developer:password@localhost:5487/mydatabase',
 *          ],
 *      ],
 * ],
 * ```
 * 
 * @property Database $database Database instance. This property is read-only
 * @property string $defaultDatabaseName Default database name.
 * @property file\Collection $fileCollection CouchDB GridFS collection instance. This property is read-only.
 * @property bool $isActive Whether the CouchDB connection is established. This property is read-only.
 * @property LogBuilder $logBuilder The log builder for this connection. Note that this property
 * differs in getter and setter. See [[getLogBuilder()]] and [[setLogBuilder()]] for details.
 * @property QueryBuilder $queryBuilder The query builder for the CouchDB connection. Note that the type
 * of this property differs in getter and setter. See [[getQueryBuilder()]] and [[setQueryBuilder()]] for
 * details.
 * 
 * @author Dan Orton <dan.orton84@gmail.com>
 */
class Connection extends Component
{
    /**
     * @event Event an event that is triggered after a DB connection is established
     */
    const EVENT_AFTER_OPEN = 'afterOpen';
    
    /**
     *
     * @var string host:port
     * 
     * Correct syntax is:
     * http[s]://[username:password]@hostname[:port][/dbname]
     * For example:
     * http://localhost:5487
     * https://developer:password@localhost:5487
     * https://developer:password@localhost:5487/mydatabase
     */
    public $dsn;
    
    /**
     *
     * @var array connection options
     * For example: 
     * 
     * ```php
     * [
     * 
     * ]
     * ```
     */
    public $options = [];
    
    /**
     *
     * @var Manager CouchDB driver manager. 
     */
    public $manager;
    
    /**
     *
     * @var bool whether to log command and query executions.
     * When enabled this option may reduce performance. CouchDB commands may contain large data,
     * consuming both CPU and memory.
     * It makes sense to disable this option in the production environment.
     */
    public $enableLogging = true;
    
    /**
     *
     * @var bool whether to enable profiling the commands and queries being executed.
     * This option will have no effect if [[enableLogging]] is disabled. 
     */
    public $enableProfiling = true;
    
    /**
     *
     * @var string name of the protocol, which should be used for the GridFS stream wrapper.
     * Only alphanumeric values are allowed: do not use any URL special characters, such as '/', '&', ':' etc.
     * @see \yii\couchdb\file\StreamWrapper
     */
    public $fileStreamProtocol = 'gridfs';
    
    /**
     *
     * @var string name of the class, which should serve as a stram wrapper for [[fileStreamProtocol]] protocol. 
     */
    public $fileStreamWrapperClass = 'yii\couchdb\file\StreamWrapper';
    
    /**
     * @var string name of the CouchDB database to use by default. 
     * If this field is left blank, connection instance will attempt to determine it from 
     * [[dsn]] automatically, if needed
     */
    private $_defaultDatabaseName;
    
    /**
     *
     * @var Database[] list of CouchDB databases 
     */
    private $_databases = [];
    
    /**
     *
     * @var QueryBuilder|array|string the query builder for this connection. 
     */
    private $_queryBuilder = 'yii\couchdb\QueryBuilder';
    
    /**
     *
     * @var LogBuilder|array|string log entries builder used for this connection. 
     */
    private $_logBuilder = 'yii\couchdb\LogBuilder';
    
    /**
     *
     * @var bool whether GridFS stream wrapper has already been registered. 
     */
    private $_fileStreamWrapperRegistered = false;
    
    /**
     * Sets default database name.
     * @param string $name  default database name
     */
    public function setDefaultDatabaseName($name)
    {
        $this->_defaultDatabaseName = $name;
    }
    
     /**
     * Returns default database name, if it is not set,
     * attempts to determine it from [[dsn]] value.
     * @return string default database name
     * @throws \yii\base\InvalidConfigException if unable to determine default database name.
     */
    public function getDefaultDatabaseName()
    {
        if ($this->_defaultDatabaseName === null) {
            if (preg_match('/^mongodb:\\/\\/.+\\/([^?&]+)/s', $this->dsn, $matches)) {
                $this->_defaultDatabaseName = $matches[1];
            } else {
                throw new InvalidConfigException("Unable to determine default database name from dsn.");
            }
        }

        return $this->_defaultDatabaseName;
    }
    
    /**
     * Returns the query builder for the this MongoDB connection.
     * @return QueryBuilder the query builder for the this MongoDB connection.
     */
    public function getQueryBuilder()
    {
        if (!is_object($this->_queryBuilder)) {
            $this->_queryBuilder = Yii::createObject($this->_queryBuilder, [$this]);
        }
        return $this->_queryBuilder;
    }
    
    /**
     * Sets the query builder for the this CouchDB connection.
     * @param QueryBuilder|array|string|null $queryBuilder the query builder for this CouchDB connection.
     */
    public function setQueryBuilder($queryBuilder)
    {
        $this->_queryBuilder = $queryBuilder;
    }
    
    /**
     * Returns log builder for this connection.
     * @return LogBuilder the log builder for this connection.
     */
    public function getLogBuilder()
    {
        if (!is_object($this->_logBuilder)) {
            $this->_logBuilder = Yii::createObject($this->_logBuilder);
        }
        return $this->_logBuilder;
    }
    
    /**
     * Sets log builder used for this connection.
     * @param array|string|LogBuilder $logBuilder the log builder for this connection.
     */
    public function setLogBuilder($logBuilder)
    {
        $this->_logBuilder = $logBuilder;
    }
    
    /**
     * Returns the MongoDB database with the given name.
     * @param string|null $name database name, if null default one will be used.
     * @param bool $refresh whether to reestablish the database connection even, if it is found in the cache.
     * @return Database database instance.
     */
    public function getDatabase($name = null, $refresh = false)
    {
        if ($name === null) {
            $name = $this->getDefaultDatabaseName();
        }
        if ($refresh || !array_key_exists($name, $this->_databases)) {
            $this->_databases[$name] = $this->selectDatabase($name);
        }

        return $this->_databases[$name];
    }

    /**
     * Selects the database with given name.
     * @param string $name database name.
     * @return Database database instance.
     */
    protected function selectDatabase($name)
    {
        return Yii::createObject([
            'class' => 'yii\couchdb\Database',
            'name' => $name,
            'connection' => $this,
        ]);
    }

    /**
     * Returns the CouchDB collection with the given name.
     * @param string|array $name collection name. If string considered as the name of the collection
     * inside the default database. If array - first element considered as the name of the database,
     * second - as name of collection inside that database
     * @param bool $refresh whether to reload the collection instance even if it is found in the cache.
     * @return Collection CouchDB collection instance.
     */
    public function getCollection($name, $refresh = false)
    {
        if (is_array($name)) {
            list ($dbName, $collectionName) = $name;
            return $this->getDatabase($dbName)->getCollection($collectionName, $refresh);
        }
        return $this->getDatabase()->getCollection($name, $refresh);
    }
    
    /**
     * Returns the CouchDB GridFS collection.
     * @param string|array $prefix collection prefix. If string considered as the prefix of the GridFS
     * collection inside the default database. If array - first element considered as the name of the database,
     * second - as prefix of the GridFS collection inside that database, if no second element present
     * default "fs" prefix will be used.
     * @param bool $refresh whether to reload the collection instance even if it is found in the cache.
     * @return file\Collection Mongo GridFS collection instance.
     */
    public function getFileCollection($prefix = 'fs', $refresh = false)
    {
        if (is_array($prefix)) {
            list ($dbName, $collectionPrefix) = $prefix;
            if (!isset($collectionPrefix)) {
                $collectionPrefix = 'fs';
            }

            return $this->getDatabase($dbName)->getFileCollection($collectionPrefix, $refresh);
        }
        return $this->getDatabase()->getFileCollection($prefix, $refresh);
    }
    
    /**
     * Returns a value indicating whether the CouchDB connection is established.
     * @return bool whether the Mongo connection is established
     */
    public function getIsActive()
    {
        return is_object($this->manager) && $this->manager->getServers() !== [];
    }
    
    /**
     * Establishes a CouchDB connection.
     * It does nothing if a CouchDB connection has already been established.
     * @throws Exception if connection fails
     */
    public function open()
    {
        if ($this->manager === null) {
            if (empty($this->dsn)) {
                throw new InvalidConfigException($this->className() . '::dsn cannot be empty.');
            }
            $token = 'Opening CouchDB connection: ' . $this->dsn;
            try {
                Yii::trace($token, __METHOD__);
                Yii::beginProfile($token, __METHOD__);
                $options = $this->options;

                $this->manager = new Manager($this->dsn, $options, $this->driverOptions);
                $this->manager->selectServer($this->manager->getReadPreference());

                $this->initConnection();
                Yii::endProfile($token, __METHOD__);
            } catch (\Exception $e) {
                Yii::endProfile($token, __METHOD__);
                throw new Exception($e->getMessage(), (int) $e->getCode(), $e);
            }

            $this->typeMap = array_merge(
                $this->typeMap,
                [
                    'root' => 'array',
                    'document' => 'array'
                ]
            );
        }
    }
    
    /**
     * Closes the currently active DB connection.
     * It does nothing if the connection is already closed.
     */
    public function close()
    {
        if ($this->manager !== null) {
            Yii::trace('Closing CouchDB connection: ' . $this->dsn, __METHOD__);
            $this->manager = null;
            foreach ($this->_databases as $database) {
                $database->clearCollections();
            }
            $this->_databases = [];
        }
    }
    
     /**
     * Initializes the DB connection.
     * This method is invoked right after the DB connection is established.
     * The default implementation triggers an [[EVENT_AFTER_OPEN]] event.
     */
    protected function initConnection()
    {
        $this->trigger(self::EVENT_AFTER_OPEN);
    }

    /**
     * Creates CouchDB command.
     * @param array $document command document contents.
     * @param string|null $databaseName database name, if not set [[defaultDatabaseName]] will be used.
     * @return Command command instance.
     */
    public function createCommand($document = [], $databaseName = null)
    {
        return new Command([
            'db' => $this,
            'databaseName' => $databaseName,
            'document' => $document,
        ]);
    }

    /**
     * Registers GridFS stream wrapper for the [[fileStreamProtocol]] protocol.
     * @param bool $force whether to enforce registration even wrapper has been already registered.
     * @return string registered stream protocol name.
     */
    public function registerFileStreamWrapper($force = false)
    {
        if ($force || !$this->_fileStreamWrapperRegistered) {
            /* @var $class \yii\couchdb\file\StreamWrapper */
            $class = $this->fileStreamWrapperClass;
            $class::register($this->fileStreamProtocol, $force);

            $this->_fileStreamWrapperRegistered = true;
        }

        return $this->fileStreamProtocol;
    }
}

