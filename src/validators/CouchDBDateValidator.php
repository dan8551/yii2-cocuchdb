<?php

namespace yii\couchdb\validators;

use yii\validators\DateValidator;

/**
 * CouchDBDateValidator is an enhanced version of [[DateValidator]].
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
 *             ['date', 'yii\couchdb\validators\CouchDBDateValidator', 'format' => 'MM/dd/yyyy']
 *         ];
 *     }
 * }
 * ```
 *
 * @see DateValidator
 *
 * @author Paul Klimov <klimov.paul@gmail.com>
 * @since 2.0.4
 */
class CouchDBDateValidator extends DateValidator
{
    /**
     * @var string the name of the attribute to receive the parsing result.
     * When this property is not null and the validation is successful, the named attribute will
     * receive the parsing result.
     *
     * This can be the same attribute as the one being validated. If this is the case,
     * the original value will be overwritten with the value after successful validation.
     */
    public $dateAttribute;


    /**
     * {@inheritdoc}
     */
    public function validateAttribute($model, $attribute)
    {
        $dateAttribute = $this->dateAttribute;
        if ($this->timestampAttribute === null) {
            $this->timestampAttribute = $dateAttribute;
        }

        $originalErrorCount = count($model->getErrors($attribute));
        parent::validateAttribute($model, $attribute);
        $afterValidateErrorCount = count($model->getErrors($attribute));

        if ($originalErrorCount === $afterValidateErrorCount) {
            if ($this->dateAttribute !== null) {
                $timestamp = $model->{$this->timestampAttribute};
                $dateAttributeValue = $model->{$this->dateAttribute};
                // ensure "dirty attributes" support :
                if (!($dateAttributeValue instanceof UTCDateTime) || $dateAttributeValue->sec !== $timestamp) {
                    $model->{$this->dateAttribute} = new UTCDateTime($timestamp * 1000);
                }
            }
        }
    }

    /**
     * {@inheritdoc}
     */
    protected function parseDateValue($value)
    {
        return $value instanceof UTCDateTime
            ? $value->toDateTime()->getTimestamp()
            : parent::parseDateValue($value);
    }
}