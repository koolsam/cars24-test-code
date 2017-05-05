<?php

namespace cars24\couchbase;

use yii\base\Component;
use yii\db\Exception;
use yii\helpers\Inflector;
use common\Util\CommonHelper;

/**
 * Connection class file
 *
 * Connection represents a connection to a couchbase cluster
 *
 * After the Couchbase connection is established,
 * one can execute any PHP API db_couchbase function
 * using Yii::$app->db_couchbase->getCluster()
 *
 * @author Ivo Pereira
 * @version 0.1
 */
class Connection extends Component {

    /**
     * connection dsn of one of the nodes,
     * the php client will determine if there are multiple nodes in the cluster
     * and hit the correct node
     */
    public $connectionString;

    /**
     * @var string the username for establishing DB connection. Defaults to empty string.
     */
    public $username = '';

    /**
     * @var string the password for establishing DB connection. Defaults to empty string.
     */
    public $password = '';

    /**
     * @var string bucket name the couchbase default bucket name, other buckets can be used
     * as usual with getCluster()->openBucket(...)
     */
    public $bucketName;

    /**
     * @var string bucket password the couchbase default bucket name password
     */
    public $bucketPassword;
    private $classNameCouchbaseCluster = 'CouchbaseCluster';

    /**
     * @var boolean whether the database connection should be automatically established
     * the component is being initialized. Defaults to true. Note, this property is only
     * effective when the object is used as an application component.
     */
    public $autoConnect = true;
    private $_cluster = null;
    private $_attributes = array();
    private $_active = false;
    private $_buckets = array();

    /**
     * Close the connection when serializing.
     * @return array
     */
    public function __sleep() {
        $this->close();
        return array_keys(get_object_vars($this));
    }

    /**
     * Initializes the component.
     * This method is required by {@link IApplicationComponent} and is invoked by application
     * when the Connection is used as an application component.
     * If you override this method, make sure to call the parent implementation
     * so that the component can be marked as initialized.
     */
    public function init() {
        parent::init();
        if ($this->autoConnect)
            $this->setActive(true);
    }

    /**
     * Returns whether the couchbase connection is established.
     * @return boolean whether the couchbase connection is established
     */
    public function getActive() {
        return $this->_active;
    }

    /**
     * Open or close the couchbase connection.
     * @param boolean $value whether to open or close couchbase connection
     * @throws InvalidConfigException if connection fails
     */
    public function setActive($value) {
        if ($value != $this->_active) {
            if ($value)
                $this->open();
            else
                $this->close();
        }
    }

    /**
     * Opens couchbase cluster connection if it is currently not
     * @throws InvalidConfigException if connection fails
     */
    protected function open() {
        if ($this->_cluster === null) {
            if (empty($this->connectionString)) {
                CommonHelper::logCBErrorInMemory();
                throw new \yii\base\Exception('Connection.connectionString cannot be empty.');
            }

            try {
                \Yii::trace('Opening CouchBase connection', __FILE__);
                // Connect to Couchbase Server

                $this->_cluster = new \CouchbaseCluster($this->connectionString, $this->username, $this->password);
                $this->_active = true;
                if (!is_object($this->_cluster)) {
                    CommonHelper::logCBErrorInMemory();
                    throw new \yii\base\Exception('Connection failed to open the Couchbase connection');
                }
            } catch (\Exception $e) {
                if (YII_DEBUG) {
                    CommonHelper::logCBErrorInMemory();
                    throw new \yii\base\Exception('Connection failed to open the connection: ' . $e->getMessage());
                } else {
                    CommonHelper::logCBErrorInMemory();
                    throw new \yii\base\Exception('CConnection failed to open the Couchbase connection.');
                }
            }
        }
    }

    /**
     * Closes the currently active Couchbase connection.
     * It does nothing if the connection is already closed.
     */
    protected function close() {
        Yii::trace('Closing Couchbase connection', __FILE__);
        $this->_cluster = null;
        $this->_buckets = array();
        $this->_active = false;
    }

    /**
     * Returns the cluster instance.
     * @return cluster the CouchBaseCluster instance, null if the connection is not established yet
     */
    public function getCluster() {
        return $this->_cluster;
    }

    public function getBucket($bucketName = '', $password = '') {
        //use default bucket name and password if blank

        if (!$bucketName) {
            if (!empty($this->bucketName)) {

                $bucketName = $this->bucketName;
                $password = $this->bucketPassword;
            } else {
                CommonHelper::logCBErrorInMemory();
                throw new \yii\base\Exception('Connection default bucket was not defined in couchbase config.');
            }
        }
        if (isset($this->_buckets[$bucketName])) {
            return $this->_buckets[$bucketName];
        }
        try {
            $this->_buckets[$bucketName] = $this->_cluster->openBucket($bucketName, $password);
        } catch (\Exception $e) {
            CommonHelper::logCBErrorInMemory();
            throw new \yii\base\Exception('Connection failed to open the Couchbase bucket connection.' . $e->getMessage());
        }

        return $this->_buckets[$bucketName];
    }


    public function __call($method, $args) {
        parent::__call($method, $args);
        if (!class_exists($this->classNameCouchbaseCluster)) {
            CommonHelper::logCBErrorInMemory();
            throw new \yii\base\Exception($this->classNameCouchbaseCluster . ' is not installed in PHP. Please install it before proceeding.');
        }
        if (!method_exists($this, $method) && method_exists($this->_cluster, $method)) {
            call_user_func_array([$this->_cluster, $method], $args);
        } elseif (method_exists($this, $method)) {
            call_user_func_array([$this, $method], $args);
        } else {
            CommonHelper::logCBErrorInMemory();
            throw new \yii\base\Exception("Method " . $method . " doesn't exist in this class. Please check for mispelling errors.");
        }
    }

}
