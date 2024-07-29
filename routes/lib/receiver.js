const queryconfig = require('../../config/query/receive_query');

const pool = require('../../config/connectionPool');
const request = require('request');
const mysql = require('mysql');

const moment = require('moment');
require('moment-timezone');
moment.tz.setDefault('Asia/Seoul');

const receiver = {
    gasList: {},
    receiveGas: {},
    errorList: {},
    timeoutCount: 20000,
    initFindByOnSensor() {
        const _this = this;
        const _query = queryconfig.findByOnSensor(); // start_time이 Null이 아닌 센서 조회(현재 사용 중인 센서)
        pool.getConnection((err, connection) => {
            if (err) {
                console.log(err);
                throw err;
            } else {
                //커넥션 사용
                connection.query(_query, (err, results) => {
                    if (err) {
                        throw err;
                    } else {
                        for (i in results) {
                            const {
                                sensor_index,
                                o2_value,
                                h2s_value,
                                voc_value,
                                comb_value,
                                co_value,
                                o2_state_code,
                                h2s_state_code,
                                voc_state_code,
                                comb_state_code,
                                co_state_code,
                                sensor_start_time,
                                local_index,
                            } = results[i];

                            let valueObj = {
                                O2: o2_value,
                                H2S: h2s_value,
                                VOC: voc_value,
                                COMB: comb_value,
                                CO: co_value,
                                o2_state_code,
                                h2s_state_code,
                                voc_state_code,
                                comb_state_code,
                                co_state_code,
                                local_index,
                            };

                            const initOnSensor = {
                                sensor_index,
                                device_index: 1,
                                start_time: moment(sensor_start_time).format(
                                    'YYYY-MM-DD HH:mm:ss'
                                ),
                                isUsedTime: true,
                                warmingup_time: null,
                                value: valueObj,
                                timeoutId: undefined,
                            };

                            // if (
                            //     !_this.receiveGas.hasOwnProperty(sensor_index)
                            // ) {
                            //     _this.receiveGas[sensor_index] = initOnSensor;
                            // }
                        }
                    }
                });
                //커넥션 반환( 커넥션 종료 메소드가 커넥션과 다르다 )
            }
            if (connection?.release) connection.release();
        });
    },
    findBygas() {
        const _this = this;
        const _query = queryconfig.findBygas();
        //풀에서 컨넥션 획득
        pool.getConnection((err, connection) => {
            if (err) {
                console.log(err);
                throw err;
            } else {
                //커넥션 사용
                connection.query(_query, (err, results) => {
                    if (err) {
                        throw err;
                    } else {
                        for (i in results) {
                            const {
                                sensor_index,
                                gas_id: id,
                                gas_code,
                                gas_name,
                                gas_unit: unit,
                                range_min,
                                range_max,
                                normal_low,
                                normal_high,
                                warning1_low,
                                warning1_high,
                                warning2_low,
                                warning2_high,
                                danger1_low,
                                danger1_high,
                                danger2_low,
                                danger2_high,
                            } = results[i];
                            const sensorIdx = sensor_index;
                            const code = gas_code;
                            const hasIdx =
                                _this.gasList.hasOwnProperty(sensorIdx);

                            if (!hasIdx) {
                                _this.gasList[sensorIdx] = {};
                                _this.gasList[sensorIdx][code] = {
                                    id,
                                    sensor_index,
                                    unit,
                                    range_min,
                                    range_max,
                                    normal_low,
                                    normal_high,
                                    warning1_low,
                                    warning1_high,
                                    warning2_low,
                                    warning2_high,
                                    danger1_low,
                                    danger1_high,
                                    danger2_low,
                                    danger2_high,
                                    normal_range: `${normal_low}-${normal_high}${unit}`,
                                };
                            } else {
                                _this.gasList[sensorIdx][code] = {
                                    id,
                                    sensor_index,
                                    unit,
                                    range_min,
                                    range_max,
                                    normal_low,
                                    normal_high,
                                    warning1_low,
                                    warning1_high,
                                    warning2_low,
                                    warning2_high,
                                    danger1_low,
                                    danger1_high,
                                    danger2_low,
                                    danger2_high,
                                    normal_range: `${normal_low}-${normal_high}${unit}`,
                                };
                            }
                        }
                    }
                });
                // connection.release();
            }
            //커넥션 반환( 커넥션 종료 메소드가 커넥션과 다르다 )
            if (connection?.release) connection.release();
        });
    },
    receive(data) {
        const _this = this;
        const { sensor_index: sensorIndex, value: gasObj } = data;
        const gasList = _this.gasList[sensorIndex];
        // const gasObj = value;
        for (gas_type in gasObj) {
            const gasInfo = gasList[gas_type];
            let _gasErrorObj = {};
            let _stateCode = 0;
            const {
                normal_low: normalLow = null,
                normal_high: normalHigh = null,
                warning1_low: warning1_Low = null,
                warning1_high: warning1_High = null,
                // TODO: 산소만 있는 수치? 1와2의 차이는?
                /**
                 * @description
                 *  가스 5종 중 산소를 제외한 나머지 가스는 안전범위 0~30 / 0~100
                 *  산소 안전범위는 18~23.5 기준
                 *  산소 기준 18미만일 떄는 warning1 / danger1 로 구분
                 *       기준 23.5 초과 일때는 warning2 / danger2  로 구분
                 *
                 */
                warning2_low: warning2_Low = null,
                warning2_high: warning2_High = null,
                danger1_low: danger1_Low = null,
                danger1_high: danger1_High = null,
                danger2_low: danger2_Low = null,
                danger2_high: danger2_High = null,
            } = gasInfo;
            const value = gasObj[gas_type];
            if (gas_type === 'O2') {
                let o2_stateCode;
                if (
                    (value >= normalLow && value <= normalHigh) ||
                    value === 0
                ) {
                    _stateCode = 0;
                } else if (
                    (value > danger1_Low && value <= danger1_High) ||
                    (value > danger2_Low &&
                        value <= danger2_High &&
                        value !== 0)
                ) {
                    _stateCode = 2;
                }
                data['value']['o2_state_code'] = _stateCode;

                // alarmHandler ///////////////////////////////
                _gasErrorObj = {
                    gas_type: 'O2',
                    stateCode: _stateCode,
                    sensorIndex,
                    recordTime: data['record_time'],
                    value,
                };

                // end alarmHandle ///////////////////////////////////
            } else {
                const _lowerCode = gas_type.toLowerCase();

                if (value >= normalLow && value <= normalHigh) {
                    _stateCode = 0;
                } else if (value >= warning1_Low && value <= warning1_High) {
                    _stateCode = 1;
                } else if (value >= danger1_Low && value <= danger1_High) {
                    _stateCode = 2;
                }

                data['value'][`${_lowerCode}_state_code`] = _stateCode;

                //alarmInsert
                // alarmHandler ///////////////////////////////
                _gasErrorObj = {
                    gas_type: gas_type,
                    stateCode: _stateCode,
                    sensorIndex,
                    recordTime: data['record_time'],
                    value,
                };
            }

            // if (_stateCode === 2) _this.alarmHandler(_gasErrorObj);
            _this.alarmHandler(_gasErrorObj);
        }
        if (_this.receiveGas?.[data.sensor_index]?.timeoutId) {
            // TODO: 설명 필요
            clearTimeout(_this.receiveGas[data.sensor_index].timeoutId);
        }
        _this.logInsert(data);
    },
    logInsert(data) {
        //수신 로그 입력

        const _this = this;
        let _query = queryconfig.logInsert(data);
        //풀에서 컨넥션 획득
        pool.getConnection((err, connection) => {
            if (err) {
                console.log(err);
                throw err;
            } else {
                //커넥션 사용
                connection.query(_query, (err, results) => {
                    if (err) {
                        // throw err;
                        console.error(err);
                    } else {
                        _this.receiveGas = {
                            ..._this.receiveGas,
                            [data.sensor_index]: {
                                ...data,
                                // TODO: from here, 모든 내용 설명 필요
                                timeoutId: setTimeout(() => {
                                    //     // TODO: usedHis table 가 어떤 역할을 하는지 설명 필요
                                    //     /**
                                    //      *  현재는 운영을 안함
                                    //      *  가스 데이터가 20초 동안 요청 되지 않으면
                                    //      *  가스 사용 이력(DB:used_his table) 업데이트 발생
                                    //      *  가스가 정상적으로 요청이 들어오면 LINE 262 clearTimeout
                                    //      */
                                    //     _this.usedHisUpdate(
                                    //         _this.receiveGas[data.sensor_index]
                                    //     );
                                    delete _this.receiveGas[data.sensor_index];

                                    //     // 가스센서가 위험 상태 였다면, 알람 이력(DB: log_gas_alarm table) UPDATE
                                    //     if (_this.errorList?.[data.sensor_index]) {
                                    //         const gasErrorObj =
                                    //             _this.errorList?.[
                                    //                 data.sensor_index
                                    //             ];
                                    //         const errorKeys =
                                    //             Object.keys(gasErrorObj);
                                    //         const errorObjLength = errorKeys.length;
                                    //         if (errorObjLength > 0) {
                                    //             for (let keys in gasErrorObj) {
                                    //                 gasErrorObj[keys][
                                    //                     'restore_time'
                                    //                 ] = data['record_time'];

                                    //                 _this.alarmUpdate(
                                    //                     gasErrorObj[keys]
                                    //                 );
                                    //             }
                                    //         }
                                    //     }
                                }, _this.timeoutCount),
                            },
                            // TODO: to here
                        };
                    }
                });
            }
            //커넥션 반환( 커넥션 종료 메소드가 커넥션과 다르다 )
            if (connection?.release) connection.release();
        });
    },
    alarmHandler(data) {
        const _this = this;
        let { stateCode, sensorIndex, recordTime, value, gas_type } = data;
        const _GasError = _this?.errorList?.[sensorIndex]?.[gas_type];
        if (stateCode === 2) {
            if (!_GasError) {
                const _gasErrorObj = {
                    gas_type,
                    record_time: recordTime,
                    sensor_index: sensorIndex,
                    device_index: 1,
                    state_code: stateCode,
                    value: value,
                    max_value: value,
                    max_record_time: recordTime,
                    dangerTimer: setInterval(() => {
                        // TODO: 5분마다 실행하는 이유?
                        /**
                         * 이슈: 가스 수치가 위험범위 내외로 수신되면 위험 상황 문자 및 알람 이력이 갱신
                         * 대안 1): dnagerTimer
                         *  가스 위험이 발생하면 5분간 안전범위로 내려와도 위험 상황 유지하기 위한 처리
                         */
                        const _gasType = gas_type.toLowerCase();
                        const { sensor_index, value } = _this.receiveGas;
                        const _stateCode =
                            _this?.receiveGas?.[sensorIndex]?.value[
                                `${_gasType}_state_code`
                            ] ?? null;
                        if (_stateCode === 0) {
                            console.log('SetInterval!!!!!');

                            _this.alarmUpdate(
                                _this.errorList[sensorIndex][gas_type]
                            );
                        } else {
                            return;
                        }
                    }, 600000),
                    safetyCount: 0,
                };

                // console.log('22-->', _this.errorList);
                _this.errorList = {
                    ..._this.errorList,
                    [sensorIndex]: {
                        ..._this.errorList[sensorIndex],
                        [gas_type]: _gasErrorObj,
                    },
                };
                _this.alarmInsert(_gasErrorObj);
            } else {
                const _tempErrorObj = {};
                let maxValue = _GasError.max_value;
                if (gas_type === 'O2') {
                    if (value < 18 && value < maxValue) {
                        _tempErrorObj.max_record_time = recordTime;
                        _tempErrorObj.max_value = value;
                    } else if (value > 23.5 && value > maxValue) {
                        _tempErrorObj.max_record_time = recordTime;
                        _tempErrorObj.max_value = value;
                    }
                } else {
                    if (value > maxValue) {
                        _tempErrorObj.max_value = value;
                        _tempErrorObj.max_record_time = recordTime;
                    }
                }

                if (!_this.errorList?.[sensorIndex]?.[gas_type]) {
                    _this.errorList = {
                        ..._this.errorList,
                        [sensorIndex]: {
                            ..._this.errorList[sensorIndex],
                            [gas_type]: {
                                ..._GasError,
                                ..._tempErrorObj,
                                safetyCount: 0,
                            },
                        },
                    };
                } else {
                    if (
                        _this.errorList[sensorIndex][gas_type].safetyCount !== 0
                    ) {
                        _this.errorList[sensorIndex][gas_type].safetyCount = 0;
                    }
                }
            }
        } else if (stateCode === 0 && _GasError) {
            if (
                _GasError?.dangerTimer &&
                _this.errorList?.[sensorIndex]?.[gas_type]
            ) {
                // TODO: 150번 카운트 하는 이유?
                /**
                 * 이슈: 가스 수치가 위험범위 내외로 수신되면 위험 상황 문자 및 알람 이력이 갱신
                 * 대안 2): _safetyCount
                 *  가스가 위험 수치 상태가 발생하고 안전범위(state_code===0)인 데이터가 150번 카운트 되면, 위험 상황이 종료로 판단해 알람이력 업데이트
                 *  _safetyCount가 150번 카운트 되기 전에 위험범위(state_code===2)인 데이터가 발생되면, _safetyCount=0으로 초기화.
                 */
                const _safetyCount =
                    _this.errorList[sensorIndex][gas_type].safetyCount + 1;
                _this.errorList[sensorIndex][gas_type].safetyCount =
                    _safetyCount;

                if (_safetyCount > 150) {
                    _this.alarmUpdate(_this.errorList[sensorIndex][gas_type]);
                }
            }
            // 위험--> 정상
            // _this.alarmUpdate(_this.errorList[sensorIndex][gas_type]);
            // delete _this.errorList[sensorIndex][gas_type];

            // if (Object.keys(_this.errorList[sensorIndex]).length === 0)
            //     delete _this.errorList[sensorIndex];
        }
    },
    usedHisUpdate(data) {
        if (!data?.sensor_index) return;
        const { sensor_index = null } = data;
        if (!sensor_index) return;
        let _query = queryconfig.usedHisUpdate({
            ...data,
            stop_time: moment().format('YYYY-MM-DD HH:mm:ss'),
        });
        pool.getConnection((err, connection) => {
            if (err) {
                console.log(err);
                throw err;
            } else {
                //커넥션 사용
                connection.query(_query, (err, results) => {
                    if (err) {
                        throw err;
                    } else {
                        console.log('SENSOR OFF!!!');
                    }
                });
                //커넥션 반환( 커넥션 종료 메소드가 커넥션과 다르다 )
            }
            if (connection?.release) connection.release();
        });
    },
    alarmInsert(data) {
        const _this = this;
        const {
            gas_type,
            record_time,
            sensor_index,
            value,
            state_code,
            device_index,
        } = data;
        const _recordTime = moment(record_time).format('YYYY-MM-DD HH:mm:ss');
        const insertData = {
            record_time: _recordTime,
            sensor_index,
            device_index,
            init_value: value,
            state_code,
            gas_type,
            danger_record_time: _recordTime,
        };
        const _query = queryconfig.alarmHisInsert(insertData);

        pool.getConnection((err, connection) => {
            if (err) {
                console.log(err);
                throw err;
            } else {
                //커넥션 사용
                connection.query(_query, (err, results) => {
                    if (err) {
                        throw err;
                    } else {
                        console.log('ERROR OCCURE!!!');
                        _this.receiverList(data);
                    }
                });
                //커넥션 반환( 커넥션 종료 메소드가 커넥션과 다르다 )
            }
            if (connection?.release) connection.release();
        });
    },
    alarmUpdate(data) {
        const _this = this;
        const {
            sensor_index,
            max_value,
            max_record_time,
            record_time,
            state_code,
            gas_type,
        } = data;
        const _recordTime = moment().format('YYYY-MM-DD HH:mm:ss');
        const updateData = {
            record_time: moment(record_time).format('YYYY-MM-DD HH:mm:ss'),
            restore_time: _recordTime,
            danger_record_time: record_time,
            danger_restore_time: _recordTime,
            sensor_index,
            max_value,
            max_record_time: max_record_time,
            gas_type,
        };
        // TODO: alarm 해제?
        const _query = queryconfig.alarmHisRestore(updateData);
        pool.getConnection((err, connection) => {
            if (err) {
                console.log(err);
                throw err;
            } else {
                //커넥션 사용
                connection.query(_query, (err, results) => {
                    if (err) {
                        throw err;
                    } else {
                        console.log('ERROR OCCURE!!!');
                        if (
                            _this.errorList[sensor_index][gas_type].dangerTimer
                        ) {
                            // TODO: alarm 해제하면 즉시 interval 해제?
                            clearInterval(
                                _this.errorList[sensor_index][gas_type]
                                    .dangerTimer
                            );
                        }
                        delete _this.errorList[sensor_index][gas_type];
                    }
                });
            }
            //커넥션 반환( 커넥션 종료 메소드가 커넥션과 다르다 )
            if (connection?.release) connection.release();
        });
    },
    receiverList(data) {
        const _this = this;
        const { sensor_index, gas_type, record_time, state_code, value } = data;
        const sensorIndex = sensor_index;
        const gasType = gas_type.toUpperCase();
        const recordTime = record_time;

        const _gasObj = _this.gasList[sensorIndex][gasType];
        const { normal_range, unit } = _gasObj;
        const _query = queryconfig.receiverList(sensorIndex);
        pool.getConnection((err, connection) => {
            if (err) {
                console.log(err);
                throw err;
            } else {
                //커넥션 사용
                connection.query(_query, (err, results) => {
                    if (err) {
                        throw err;
                    } else {
                        let tempArr = [];
                        for (i in results) {
                            const {
                                wk_name,
                                wk_phone,
                                sensor_name,
                                local_name,
                            } = results[i];
                            const sendObj = {
                                name: wk_name,
                                tel: wk_phone,
                                record_time: recordTime.split('.')[0],
                                sensor_name: sensor_name,
                                gas_type: gasType,
                                state_code,
                                init_value: value,
                                normal_range,
                                unit,
                                sensor_name,
                                local_name,
                            };
                            tempArr.push(sendObj);
                            _this.smsSend(sendObj);
                        }
                    }
                });
            }
            //커넥션 반환( 커넥션 종료 메소드가 커넥션과 다르다 )
            if (connection?.release) connection.release();
        });
    },
    smsSend(obj) {
        const _postURL = `${process.env.SMS_SERVER}/alarm/iggas/danger`;
        request.post(
            {
                url: _postURL,
                body: obj,
                json: true,
            },
            function (error, res, body) {}
        );
    },
};

module.exports = receiver;
