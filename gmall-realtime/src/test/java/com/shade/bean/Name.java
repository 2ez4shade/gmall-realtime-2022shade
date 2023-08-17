package com.shade.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author: shade
 * @date: 2022/7/20 19:53
 * @description:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Name {
    String id;
    String name;
    Long ts;
}
