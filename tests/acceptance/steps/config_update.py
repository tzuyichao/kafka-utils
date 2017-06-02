# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from behave import then
from behave import when
from kafka.errors import MessageSizeTooLargeError

from .util import get_cluster_config
from .util import produce_example_msg
from .util import update_topic_config
from kafka_utils.util.zookeeper import ZK


@when(u'we set the configuration of the topic to 0 bytes')
def step_impl1(context):
    context.output = update_topic_config(
        context.topic,
        'max.message.bytes=0'
    )


@then(u'we produce to a kafka topic it should fail')
def step_impl2(context):
    try:
        produce_example_msg(context.topic, num_messages=1)
        assert False, "Exception should not be raised"
    except MessageSizeTooLargeError as e:
        assert isinstance(e, MessageSizeTooLargeError)


@when(u'we change the topic config in zk to 10000 bytes for kafka 10')
def step_impl3(context):
    cluster_config = get_cluster_config()
    with ZK(cluster_config) as zk:
        current_config = zk.get_topic_config(context.topic)
        current_config['config']['max.message.bytes'] = '1000'
        zk.set_topic_config(context.topic, value=current_config)


@when(u'we change the topic config in zk to 10000 bytes for kafka 9')
def step_impl4(context):
    cluster_config = get_cluster_config()
    with ZK(cluster_config) as zk:
        current_config = zk.get_topic_config(context.topic)
        current_config['config']['max.message.bytes'] = '1000'
        zk.set_topic_config(context.topic, value=current_config, kafka_version=(0, 9, 2))


@then(u'we produce to a kafka topic it should succeed')
def step_impl5(context):
    try:
        produce_example_msg(context.topic, num_messages=1)
    except MessageSizeTooLargeError as e:
        assert False, "Exception should not be raised"
        assert isinstance(e, MessageSizeTooLargeError)
