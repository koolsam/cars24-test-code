<?php
/**
 * @link http://www.yiiframework.com/
 * @copyright Copyright (c) 2008 Yii Software LLC
 * @license http://www.yiiframework.com/license/
 */

namespace cars24\couchbase;

use Yii;
use yii\di\Instance;


class Cache extends \yii\caching\Cache
{
    protected $prefix;
    /** @var CouchbaseBucket */
    public $bucket;
    public $password;
    /** @var CouchbaseCluster */
    protected $cluster = 'couchbase';
    /**
     * LegacyCouchbaseStore constructor.
     *
     * @param CouchbaseCluster $cluster
     * @param                  $bucket
     * @param string           $password
     * @param null             $prefix
     * @param string           $serialize
     */
    
    public function init()
    {
        parent::init();
        $this->cluster = Instance::ensure($this->cluster, Connection::className());
        $this->bucket = $this->cluster->getBucket($this->bucket,$this->password);
    }
    /**
     * {@inheritdoc}
     */
    protected function getValue($key)
    { 
        try {
            $result = $this->bucket->get($this->resolveKey($key));
            return $this->getMetaDoc($result);
        } catch (\Exception $e) {
            
            return false;
        }
    }
    
    protected function getValues($keys) {
        try{
            $result = $this->bucket->get($this->resolveKey($keys));
            $returnArr = [];
            foreach($result as $key=>$value) {
               $returnArr[$key] = $this->getMetaDoc($value);
            }
            
        } catch (\Exception $e) {
            return false;
        }
        
        return $returnArr;
    }

    /**
     * Store an item in the cache if the key doesn't exist.
     *
     * @param string|array $key
     * @param mixed        $value
     * @param int          $second
     *
     * @return bool
     */
    protected function setValue($key, $value, $second = 0)
    {
        
        try {
            $options = ($second === 0) ? [] : ['expiry' => ($second)];
            $this->bucket->upsert($this->resolveKey($key), $value, ['expiry' => $second]);
            return true;
        } catch (\Exception $e) {
            \Yii::$app->log->getLogger()->log("Getting error to update key $key with data ".  json_encode($value), \yii\log\Logger::LEVEL_WARNING);
            return false;
        }
    }
    
    protected function setValues($data, $second = 0)
    {
        try {
            $options = ($second === 0) ? [] : ['expiry' => ($second)];
            foreach($data as $key=>$record) {
                $this->bucket->upsert($this->resolveKey($key), $record, $options);;
            }
            
            return true;
        } catch (\Exception $e) {
            \Yii::$app->log->getLogger()->log("Getting error to update key $key with data ".  json_encode($record), \yii\log\Logger::LEVEL_WARNING);
            return false;
        }
        
        
    }
    
    protected function addValue($key, $value, $second = 0) {
        $options = ($second === 0) ? [] : ['expiry' => ($second)];
        try {
            $this->bucket->insert($this->resolveKey($key), $value, $options);
            return true;
        } catch (\Exception $e) {
            \Yii::$app->log->getLogger()->log("Getting error to add key $key with data ".  json_encode($value), \yii\log\Logger::LEVEL_WARNING);
            return false;
        }
    }
    
    protected function addValues($data, $second = 0) {
        try {
            $options = ($second === 0) ? [] : ['expiry' => ($second)];
            foreach($data as $key=>$record) {
                $this->bucket->upsert($this->resolveKey($key), $record, $options);
            }
            
            return true;
        } catch (\Exception $e) {
            \Yii::$app->log->getLogger()->log("Getting error to add key $key with data ".  json_encode($record), \yii\log\Logger::LEVEL_WARNING);
            return false;
        }
    }
    

    protected function deleteValue($key) {
       try {
            $this->bucket->remove($this->resolveKey($key));
        } catch (\Exception $e) {
            // Ignore exceptions from remove
        } 
    }
    
    public function flushValues() {
        $result = $this->bucket->manager()->flush();        
    }
    
    private function resolveKey($keys)
    {
        if (is_array($keys)) {
            $result = [];
            foreach ($keys as $key) {
                $result[] = $this->prefix.$key;
            }
            return $result;
        }
        return $this->prefix.$keys;
    }
    
    
    protected function getMetaDoc($meta)
    {
        if ($meta instanceof \CouchbaseMetaDoc) {
            return $meta->value;
        }
        if (is_array($meta)) {
            $result = [];
            foreach ($meta as $key=>$row) {
                $result[$key] = (array)$this->getMetaDoc($row);
            }
            return $result;
        }
        return;
    }
}
