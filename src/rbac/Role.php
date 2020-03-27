<?php

namespace yii\couchdb\rbac;

/**
 * Role is a special version of [[\yii\rbac\Role]] dedicated to CouchDB RBAC implementation.
 *
 * @author Dan Orton <dan.orton84@gmail.com>
 */
class Role extends \yii\rbac\Role
{
    /**
     * @var array|null list of parent item names.
     */
    public $parents;
}