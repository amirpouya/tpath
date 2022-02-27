use std::collections:: {HashSet};
use std::hash::Hash;
use rayon::prelude::*;

use itertools::Itertools;
use crate::edge::Edge;
use crate::node::Node;
use hashbrown::HashMap;



// If you know one of the tables is smaller, it is best to make it the second parameter.

pub fn hash_join_single<A, B, K>(first: &[(K, A)], second: &[(K, B)]) -> Vec<(A, K, B)>
    where
        K: Hash + Eq + Copy +Send + Sync,
        A: Copy +Send + Sync,
        B: Copy+Send + Sync,
{
    // let  hash_map =
    //     second.par_iter()
    //         .fold(
    //             || HashMap::new(),
    //             |mut acc, &(key, val_a)| {
    //                 acc.entry(key).or_insert_with(Vec::new).push(val_a);
    //                 acc
    //             },
    //         )
    //         .reduce(
    //             || HashMap::new(),
    //             |m1, m2| {
    //                 m2.iter().fold(m1, |mut acc, (k, vs)| {
    //                     acc.entry(k.clone()).or_insert(vec![]).extend(vs);
    //                     acc
    //                 })
    //             },
    //         );
    //

    let mut hash_map = HashMap::new();

    // hash phase
    for &(key, val_a) in second {
        // collect all values by their keys, appending new ones to each existing entry
        hash_map.entry(key).or_insert_with(Vec::new).push(val_a);
    }

    let mut result = Vec::new();
    // join phase
    for &(key, val_b) in first {
        if let Some(vals) = hash_map.get(&key) {
            let tuples = vals.iter().map(|&val_a| (val_b, key, val_a));
            result.extend(tuples);
        }
    }
    result
}


pub fn hash_join<A, B, K>(first: &[(K, A)], second: &[(K, B)]) -> Vec<(A, K, B)>
    where
        K: Hash + Eq + Copy +Send + Sync,
        A: Copy +Send + Sync ,
        B: Copy+Send + Sync ,
{

    let hash_map =
        second.par_iter()
            .fold(
                || HashMap::new(),
                |mut acc, &(key, val_a)| {
                    acc.entry(key).or_insert_with(Vec::new).push(val_a);
                    acc
                },
            )
            .reduce(
                || HashMap::new(),
                |m1, m2| {
                    m2.iter().fold(m1, |mut acc, (k, vs)| {
                        acc.entry(k.clone()).or_insert(vec![]).extend(vs);
                        acc
                    })
                },
            );

    let mut result = Vec::new();
    // join phase
    for &(key, val_b) in first {
        if let Some(vals) = hash_map.get(&key) {
            let tuples = vals.iter().map(|&val_a| (val_b, key, val_a));
            result.extend(tuples);
        }
    }
    result
}



pub fn hash_join_2<A,B>(first: & Vec<(i32, A)>, second: &Vec<(i32,  B)>) -> Vec<(A, i32, B)>

    where
        A: Copy +Send + Sync ,
        B: Copy+Send + Sync ,
{

    let hash_map =
        second.par_iter()
            .fold(
                || HashMap::new(),
                |mut acc, &(key, val_a)| {
                    acc.entry(key).or_insert_with(Vec::new).push(val_a);
                    acc
                },
            )
            .reduce(
                || HashMap::new(),
                |m1, m2| {
                    m2.iter().fold(m1, |mut acc, (k, vs)| {
                        acc.entry(k.clone()).or_insert(vec![]).extend(vs);
                        acc
                    })
                },
            );

    let mut result:Vec<(A, i32, B)> = vec![];
    let  temp:Vec<(A, i32, Vec<B>)> = first.par_iter()
        //.filter(|&(key, val_b)| hash_map.contains_key(&key))
        .filter_map(|&(key, val_b)|
                        match hash_map.get(&key) {
                            Some(x) => Some((val_b, key, hash_map.get(&key).unwrap().clone())),
                            None => { None },
                        }).collect();
    //.map(|(val_b, key,val_as)|val_as.par_iter().map(|val_a| ((val_b, key, val_a)))).collect();

    temp.iter().for_each(|(val_b, key,val_as)| val_as.iter().for_each(|val_a| result.push((*val_b, *key, *val_a))));
    result
}


pub fn hash_join_edge_node(first: & Vec<(i32, Edge)>, second: &Vec<(i32,  Node)>) -> Vec<(Edge, i32, Node)>

{

    let hash_map =
        second.par_iter()
            .fold(
                || HashMap::new(),
                |mut acc, &(key, val_a)| {
                    acc.entry(key).or_insert_with(Vec::new).push(val_a);
                    acc
                },
            )
            .reduce(
                || HashMap::new(),
                |m1, m2| {
                    m2.iter().fold(m1, |mut acc, (k, vs)| {
                        acc.entry(k.clone()).or_insert(vec![]).extend(vs);
                        acc
                    })
                },
            );

    let  temp:Vec<(Edge, i32, Vec<Node>)> =
        first.par_iter().filter(|&(key, val_b)| hash_map.contains_key(&key))
            .map(|&(key, val_b)| (val_b, key, hash_map.get(&key).unwrap().clone())).collect();
                     // .map(|&(key, val_b)| (hash_map.get(&key).unwrap().par_iter().map(move|&val_a| (val_b, key, val_a)))).flat_map(|x|x).collect();

    let mut result:Vec<(Edge, i32, Node)> = vec![];
    temp.iter().for_each(|(val_b, key,val_as)| val_as.iter().for_each(|val_a| result.push((*val_b, *key, *val_a))));
    result
}

fn anti_join<A, B, K>(first: &[(K, A)], second: &[(K, B)]) -> Vec<K>
    where
        K: Hash + Eq + Copy,
        A: Copy,
        B: Copy,
{
    let mut hash_map = HashMap::new();

    // hash phase
    for &(key, val_a) in second {
        // collect all values by their keys, appending new ones to each existing entry
        hash_map.entry(key).or_insert_with(Vec::new).push(val_a);
    }

    let mut result = Vec::new();
    // join phase
    for &(key, _) in first {
        if !hash_map.contains_key(&key){
            result.push(key);
        }
    }

    result
}

pub fn intersect(a:&Vec<i32>, b:&Vec<i32>) -> Vec<i32>
{
    let a: HashSet<i32> =a.clone().into_iter().collect();
    let b: HashSet<i32> = b.clone().into_iter().collect();

    let intersection = a.intersection(&b);
    intersection.map(|e| *e).collect_vec()
}