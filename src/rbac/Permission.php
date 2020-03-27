<?php

namespace yii\couchdb\rbac;

/**
 * Permission is a special version of [[\yii\rbac\Permission]] dedicated to CouchDB RBAC implementation.
 *
 * @author Dan Orton <dan.orton84@gmail.com>
 */
class Permission extends \yii\rbac\Permission
{
    /**
     * @var array|null list of parent item names.
     */
    public $parents;
}