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

import com.facebook.presto.bytecode.DynamicClassLoader;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.facebook.presto.operator.aggregation.AccumulatorCompiler;
import com.facebook.presto.operator.aggregation.AggregationMetadata;
import com.facebook.presto.operator.aggregation.AggregationMetadata.AccumulatorStateDescriptor;
import com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata;
import com.facebook.presto.operator.aggregation.GenericAccumulatorFactoryBinder;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.function.AccumulatorState;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.BLOCK_INDEX;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.NULLABLE_BLOCK_INPUT_CHANNEL;
import static com.facebook.presto.operator.aggregation.AggregationMetadata.ParameterMetadata.ParameterType.STATE;
import static com.facebook.presto.operator.aggregation.AggregationUtils.generateAggregationName;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class StringAggregationFunction
        extends SqlAggregationFunction
{
    private static final String NAME = "string_agg";
    private static final MethodHandle INPUT_FUNCTION = methodHandle(StringAggregationFunction.class, "input", Type.class, StringAggregationState.class, Block.class, int.class, Slice.class); ///改
    private static final MethodHandle COMBINE_FUNCTION = methodHandle(StringAggregationFunction.class, "combine", Type.class, StringAggregationState.class, StringAggregationState.class);
    private static final MethodHandle OUTPUT_FUNCTION = methodHandle(StringAggregationFunction.class, "output", Type.class, StringAggregationState.class, BlockBuilder.class);

    public static final StringAggregationFunction STRING_AGGREGATION_FUNCTION = new StringAggregationFunction();

    private StringAggregationFunction()
    {
        super(NAME,
                ImmutableList.of(typeVariable("T")), //ImmutableList.of(), //ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                VARCHAR.getTypeSignature(), ///改
                ImmutableList.of(parseTypeSignature("T"), VARCHAR.getTypeSignature())); /// 改
    }

    @Override
    public String getDescription()
    {
        return "return a string of values delimited";
    }

    @Override
    public InternalAggregationFunction specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionManager functionManager)
    {
        Type type = boundVariables.getTypeVariable("T");
        return generateAggregation(type);
    }

    private static InternalAggregationFunction generateAggregation(Type type)
    {
        DynamicClassLoader classLoader = new DynamicClassLoader(StringAggregationFunction.class.getClassLoader());

        AccumulatorStateSerializer<?> stateSerializer = new StringAggregationStateSerializer(type);
        AccumulatorStateFactory<?> stateFactory = new StringAggregationStateFactory(type);

        List<Type> inputTypes = ImmutableList.of(type, VARCHAR);
//        Type outputType = new ArrayType(type);
        Type intermediateType = stateSerializer.getSerializedType();
        List<ParameterMetadata> inputParameterMetadata = createInputParameterMetadata(type);

        MethodHandle inputFunction = INPUT_FUNCTION.bindTo(type);
        MethodHandle combineFunction = COMBINE_FUNCTION.bindTo(type);
        MethodHandle outputFunction = OUTPUT_FUNCTION.bindTo(type);
        Class<? extends AccumulatorState> stateInterface = StringAggregationState.class;

        AggregationMetadata metadata = new AggregationMetadata(
                generateAggregationName(NAME, type.getTypeSignature(), inputTypes.stream().map(Type::getTypeSignature).collect(toImmutableList())),
                inputParameterMetadata,
                inputFunction,
                combineFunction,
                outputFunction,
                ImmutableList.of(new AccumulatorStateDescriptor(
                        stateInterface,
                        stateSerializer,
                        stateFactory)),
                VARCHAR);

        GenericAccumulatorFactoryBinder factory = AccumulatorCompiler.generateAccumulatorFactoryBinder(metadata, classLoader);
        return new InternalAggregationFunction(NAME, inputTypes, ImmutableList.of(intermediateType), VARCHAR, true, true, factory);
    }

    private static List<ParameterMetadata> createInputParameterMetadata(Type value)
    {
        return ImmutableList.of(new ParameterMetadata(STATE), new ParameterMetadata(NULLABLE_BLOCK_INPUT_CHANNEL, value), new ParameterMetadata(BLOCK_INDEX), new ParameterMetadata(INPUT_CHANNEL, VARCHAR)); ///改
    }

    public static void input(Type type, StringAggregationState state, Block value, int position, Slice delimeter)
    {
        state.add(value, position);
        state.setDelimiter(delimeter);
    }

    public static void combine(Type type, StringAggregationState state, StringAggregationState otherState)
    {
        state.merge(otherState);
    }

    public static void output(Type elementType, StringAggregationState state, BlockBuilder out)
    {
        if (state.isEmpty()) {
            out.appendNull();
        }
        else {
            state.forEach((block, position) -> accept(out, elementType, state.getDelimiter(), state, block, position));
            out.closeEntry();
        }
    }

    private static void accept(BlockBuilder out, Type elementType, Slice s, StringAggregationState state, Block block, int position)
    {
        out.writeBytes(elementType.getSlice(block, position), 0, elementType.getSlice(block, position).length());
        if (state.getSize() != position + 1) {
            out.writeBytes(s, 0, s.length());
        }
    }
}
