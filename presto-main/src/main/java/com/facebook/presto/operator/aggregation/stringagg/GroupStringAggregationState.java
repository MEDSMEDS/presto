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

import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.AbstractGroupCollectionAggregationState;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

public final class GroupStringAggregationState
        extends AbstractGroupCollectionAggregationState<StringAggregationStateConsumer>
        implements StringAggregationState
{
    private static final int MAX_BLOCK_SIZE = 1024 * 1024;
    private static final int VALUE_CHANNEL = 0;
    private Slice delimiter;
    private boolean isAdd;

    GroupStringAggregationState(Type valueType)
    {
        super(PageBuilder.withMaxPageSize(MAX_BLOCK_SIZE, ImmutableList.of(valueType)));
    }

    @Override
    public final void add(Block block, int position)
    {
        prepareAdd();
        appendAtChannel(VALUE_CHANNEL, block, position);
    }

    @Override
    public void setDelimiter(Slice delimiter)
    {
        this.delimiter = delimiter;
    }

    @Override
    public Slice getDelimiter()
    {
        return delimiter;
    }

    @Override
    public int getSize()
    {
        return getEntryCount();
    }

    @Override
    public void merge(StringAggregationState otherState)
    {
        otherState.forEach(this::add);
        if (this.getDelimiter() == null && otherState.getDelimiter() != null) {
            this.setDelimiter(otherState.getDelimiter());
        }
    }

    @Override
    protected final void accept(StringAggregationStateConsumer consumer, PageBuilder pageBuilder, int currentPosition)
    {
        consumer.accept(pageBuilder.getBlockBuilder(VALUE_CHANNEL), currentPosition);
    }
}
