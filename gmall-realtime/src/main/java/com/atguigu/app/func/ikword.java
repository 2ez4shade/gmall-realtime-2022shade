package com.atguigu.app.func;

import com.atguigu.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @author: shade
 * @date: 2022/7/25 17:32
 * @description:
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class ikword extends TableFunction<Row> {

    public void eval(String s) {
        try {
            List<String> list = KeywordUtil.analyze(s);
            for (String s1 : list) {
                collect(Row.of(s1));
            }
        } catch (IOException e) {
            collect(Row.of(s));
        }
    }
}