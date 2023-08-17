package com.atguigu.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: shade
 * @date: 2022/7/25 17:43
 * @description:
 */
public class KeywordUtil {
    /**
     * 分词
     * @param word
     * @return
     * @throws IOException
     */
    public static List<String> analyze(String word) throws IOException {
        ArrayList<String> list = new ArrayList<>();

        StringReader stringReader = new StringReader(word);
        IKSegmenter ikSegmenter = new IKSegmenter(stringReader, true);

        Lexeme next = ikSegmenter.next();
        while (next!=null){
            list.add(next.getLexemeText());
            next = ikSegmenter.next();
        }
        return list;
    }
}
