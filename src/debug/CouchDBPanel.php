<?php

namespace yii\couchdb\debug;

use Yii;
use yii\debug\models\search\Db;
use yii\debug\panels\DbPanel;
use yii\log\Logger;

/**
 * CouchDBPanel panel that collects and displays CouchDB queries performed.
 *
 * @property array $profileLogs This property is read-only.
 *
 * @author Klimov Paul <klimov@zfort.com>
 * @since 2.0.1
 */
class CouchDBPanel extends DbPanel
{
    /**
     * {@inheritdoc}
     */
    public $db = 'couchdb';


    /**
     * {@inheritdoc}
     */
    public function init()
    {
        $this->actions['couchdb-explain'] = [
            'class' => 'yii\\couchdb\\debug\\ExplainAction',
            'panel' => $this,
        ];
    }

    /**
     * {@inheritdoc}
     */
    public function getName()
    {
        return 'CouchDB';
    }

    /**
     * {@inheritdoc}
     */
    public function getSummaryName()
    {
        return 'CouchDB';
    }

    /**
     * {@inheritdoc}
     */
    public function getDetail()
    {
        $searchModel = new Db();

        if (!$searchModel->load(Yii::$app->request->getQueryParams())) {
            $searchModel->load($this->defaultFilter, '');
        }

        $dataProvider = $searchModel->search($this->getModels());
        $dataProvider->getSort()->defaultOrder = $this->defaultOrder;

        return Yii::$app->view->render('@yii/couchdb/debug/views/detail', [
            'panel' => $this,
            'dataProvider' => $dataProvider,
            'searchModel' => $searchModel,
        ]);
    }

    /**
     * Returns all profile logs of the current request for this panel.
     * @return array
     */
    public function getProfileLogs()
    {
        $target = $this->module->logTarget;

        return $target->filterMessages($target->messages, Logger::LEVEL_PROFILE, [
            'yii\couchdb\Command::*',
            'yii\couchdb\Query::*',
            'yii\couchdb\BatchQueryResult::*',
        ]);
    }

    /**
     * {@inheritdoc}
     */
    protected function hasExplain()
    {
        return true;
    }

    /**
     * {@inheritdoc}
     */
    protected function getQueryType($timing)
    {
        $timing = ltrim($timing);
        $timing = mb_substr($timing, 0, mb_strpos($timing, '('), 'utf8');
        $matches = explode('.', $timing);

        return count($matches) ? array_pop($matches) : '';
    }

    /**
     * {@inheritdoc}
     */
    public static function canBeExplained($type)
    {
        return $type === 'find';
    }
}