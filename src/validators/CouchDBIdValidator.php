<?php

namespace yii\couchdb\validators;

use yii\base\InvalidConfigException;
use yii\validators\Validator;
use Yii;

/**
 * CouchDBIdValidator verifies if the attribute is a valid CouchDB ID.
 * Attribute will be considered as valid, if it is a string value.
 *
 * Usage example:
 *
 * ```php
 * class Customer extends yii\couchdb\ActiveRecord
 * {
 *     ...
 *     public function rules()
 *     {
 *         return [
 *             ['_id', 'yii\couchdb\validators\CouchDBIdValidator']
 *         ];
 *     }
 * }
 * ```
 *
 * This validator may also serve as a filter, allowing conversion of Mongo ID value either to the plain string
 * instance. You can enable this feature via [[forceFormat]].
 * 
 * @author Dan Orton <dan.orton84@gmail.com>
 */
class CouchDBIdValidator extends Validator
{
    /**
     * @var string|null specifies the format, which validated attribute value should be converted to
     * in case validation was successful.
     * valid values are:
     * - 'string' - enforce value converted to plain string.
     *   If not set - no conversion will be performed, leaving attribute value intact.
     */
    public $forceFormat;


    /**
     * {@inheritdoc}
     */
    public function init()
    {
        parent::init();
        if ($this->message === null) {
            $this->message = Yii::t('yii', '{attribute} is invalid.');
        }
    }

    /**
     * {@inheritdoc}
     */
    public function validateAttribute($model, $attribute)
    {
        $value = $model->$attribute;
        $id = $this->parseId($value);
        if (is_object($id)) {
            if ($this->forceFormat !== null) {
                switch ($this->forceFormat) {
                    case 'string' : {
                        $model->$attribute = $id->__toString();
                        break;
                    }
                    case 'object' : {
                        $model->$attribute = $id;
                        break;
                    }
                    default: {
                        throw new InvalidConfigException("Unrecognized format '{$this->forceFormat}'");
                    }
                }
            }
        } else {
            $this->addError($model, $attribute, $this->message, []);
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function validateValue($value)
    {
        return is_object($this->parseId($value)) ? null : [$this->message, []];
    }

    /**
     * @param mixed $value
     * @return ObjectID|null
     */
    private function parseId($value)
    {
        if ($value instanceof ObjectID) {
            return $value;
        }
        try {
            return new ObjectID($value);
        } catch (\Exception $e) {
            return null;
        }
    }
}