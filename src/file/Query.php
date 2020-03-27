<?php

namespace yii\couchdb\file;

use Yii;

/**
 * Query represents CouchDB "find" operation for GridFS collection.
 *
 * Query behaves exactly as regular [[\yii\couchdb\Query]].
 * Found files will be represented as arrays of file document attributes with
 * additional 'file' key, which stores [[\GridFSFile]] instance.
 *
 * @property Collection $collection Collection instance. This property is read-only.
 *
 * @author Dan Orton <dan.orton84@gmail.com>
 */
class Query extends \yii\couchdb\Query
{
    /**
     * Returns the CouchDB collection for this query.
     * @param \yii\couchdb\Connection $db CouchDB connection.
     * @return Collection collection instance.
     */
    public function getCollection($db = null)
    {
        if ($db === null) {
            $db = Yii::$app->get('couchdb');
        }

        return $db->getFileCollection($this->from);
    }
}