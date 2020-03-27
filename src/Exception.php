<?php

namespace yii\couchdb;

/**
 * Exception represents an exception that is caused by some CouchDB-related operations.
 *
 * @author Dan Orton <dan.orton84@gmail.com>
 */
class Exception extends \yii\base\Exception
{
    /**
     * @return string the user-friendly name of this exception
     */
    public function getName()
    {
        return 'CouchDB Exception';
    }
}