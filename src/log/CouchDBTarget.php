<?php

namespace yii\couchdb\log;

use yii\base\InvalidConfigException;
use yii\di\Instance;
use yii\helpers\VarDumper;
use yii\log\Target;
use yii\couchdb\Connection;

/**
 * CouchDBTarget stores log messages in a CouchDB collection.
 *
 * By default, CouchDBTarget stores the log messages in a CouchDB collection named 'log'.
 * The collection can be changed by setting the [[logCollection]] property.
 *
 * @author Dan Orton <dan.orton84@gmail.com>
 */
class CouchDBTarget extends Target
{
    /**
     * @var Connection|string the CouchDB connection object or the application component ID of the CouchDB connection.
     * After the CouchDBTarget object is created, if you want to change this property, you should only assign it
     * with a CouchDB connection object.
     */
    public $db = 'couchdb';
    /**
     * @var string|array the name of the CouchDB collection that stores the session data.
     * Please refer to [[Connection::getCollection()]] on how to specify this parameter.
     * This collection is better to be pre-created with fields 'id' and 'expire' indexed.
     */
    public $logCollection = 'log';


    /**
     * Initializes the CouchDBTarget component.
     * This method will initialize the [[db]] property to make sure it refers to a valid CouchDB connection.
     * @throws InvalidConfigException if [[db]] is invalid.
     */
    public function init()
    {
        parent::init();
        $this->db = Instance::ensure($this->db, Connection::className());
    }

    /**
     * Stores log messages to CouchDB collection.
     */
    public function export()
    {
        $rows = [];
        foreach ($this->messages as $message) {
            list($text, $level, $category, $timestamp) = $message;
            if (!is_string($text)) {
                // exceptions may not be serializable if in the call stack somewhere is a Closure
                if ($text instanceof \Throwable || $text instanceof \Exception) {
                    $text = (string) $text;
                } else {
                    $text = VarDumper::export($text);
                }
            }
            $rows[] = [
                'level' => $level,
                'category' => $category,
                'log_time' => $timestamp,
                'prefix' => $this->getMessagePrefix($message),
                'message' => $text,
            ];
        }

        $this->db->getCollection($this->logCollection)->batchInsert($rows);
    }
}