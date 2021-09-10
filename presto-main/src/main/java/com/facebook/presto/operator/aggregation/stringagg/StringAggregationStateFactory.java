/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.operator.aggregation.stringagg;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateFactory;

public class StringAggregationStateFactory
        implements AccumulatorStateFactory<StringAggregationState>
{
    private final Type type;

    public StringAggregationStateFactory(Type type)
    {
        this.type = type;
    }

    @Override
    public StringAggregationState createSingleState()
    {
        return new SingleStringAggregationState(type);
    }

    @Override
    public Class<? extends StringAggregationState> getSingleStateClass()
    {
        return SingleStringAggregationState.class;
    }

    @Override
    public StringAggregationState createGroupedState()
    {
        return new GroupStringAggregationState(type);
    }

    @Override
    public Class<? extends StringAggregationState> getGroupedStateClass()
    {
        return GroupStringAggregationState.class;
    }
}
