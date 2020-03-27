<?php

namespace dan8551\couchdb;

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
 * $connection = new \dan8551\couchdb\Connection([
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
 *              'class' => '\dan8551\couchdb\Connection',
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
 * @author Dan Orton <dan.orton84@gmail.com
 * @since 2.0
 */
class Connection extends Component{
    
}

