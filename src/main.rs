extern crate tpath;
use itertools::Itertools;

use rayon::prelude::*;

use tpath::interval;
use tpath::interval::Interval;
use tpath::node::Node;
use tpath::edge::{Edge};
use std::collections::HashMap;
use std::ptr::null;
use tpath::join::{hash_join, hash_join_edge_node, hash_join_2};
use tpath::label::Label;
use std::time::Instant;
use rayon::current_num_threads;

fn main() {


    let query_list = [1,2,3,4,5,6,7,8,9,10,11,12];
    fn log(input: String, level: usize, debug_flag: usize) {
        if debug_flag >= level {
            println!("{:?}", input);
        }
    }
    //let mut num_thred_ = current_num_threads();
    let mut config_addr = std::env::args().nth(1).unwrap_or(("/Users/amir/Documents/Projects/rust/tpath/data/contact").to_string()).parse::<String>().unwrap();
    let qt = std::env::args().nth(2).unwrap_or(("12").to_string()).parse::<String>().unwrap();
    let qtt:i32 = qt.parse().unwrap();

    let debug_flag_ = std::env::args().nth(3).unwrap_or(("").to_string()).parse::<String>().unwrap();

    let mut debug_flag = 3;
    if debug_flag_ != ""{
        debug_flag = 10;
    }
    debug_flag = 3;
    let file_name = config_addr.clone();

    let num_thred_ = std::env::args().nth(4).unwrap_or((1).to_string()).parse::<usize>().unwrap();
    rayon::ThreadPoolBuilder::new().num_threads(num_thred_ as usize).build_global().unwrap();


    //read the config
    let data_name = file_name.split("/").last().unwrap().clone();

    //let n = Node::get_from_file("/Users/amir/Documents/Projects/rust/tpath/data/simp/node.csv");
    let nodes = Node::get_from_file(&(config_addr.clone()+"/node.csv"));

    log(format!("Nodes{:?}", &nodes), 10, debug_flag);
    let exec_time = Instant::now();
    //log(format!("Node load Complete{:?}:",exec_time.elapsed().as_millis()), 0, debug_flag);
    let exec_time = Instant::now();

    //let e = Edge::get_from_file("/Users/amir/Documents/Projects/rust/tpath/data/simp/edge.csv");
     let edges = Edge::get_from_file_par(&(config_addr.clone()+"/edge.csv"));

    log(format!("Edges{:?}", &edges), 10, debug_flag);
    //log(format!("Edge load Complete{:?}:",exec_time.elapsed().as_millis()), 0, debug_flag);
    //query exec

    if query_list.contains(&1) {
        //Q1 MATCH (x: Person) ON contact_tracing
        let exec_time = Instant::now();
        let q1:Vec<&Node> = nodes.par_iter().filter(|p| matches!(p.label,Label::person)).collect();
        let struct_time = exec_time.elapsed().as_millis();
        let q1:Vec<(i32,i32)> = q1.par_iter()
            .flat_map(|p|  (p.time.start..p.time.end+1).into_par_iter().map(move |i| (p.nid,i)))
            //.flat_map(|x| x)
            .collect();

        log(format!("q1(x,t){:?}", &q1), 5, debug_flag);
        log(format!("{:?},q1 {:?},{:?},{:?}, {:?}", &num_thred_, data_name,struct_time,exec_time.elapsed().as_millis(), &q1.len()), 1, debug_flag);
    }
    if query_list.contains(&2) {
        //Q2 MATCH (x: Person { risk = 'low '}) ON contact_tracing
        let exec_time = Instant::now();
        let q2: Vec<&Node> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::low)).collect();
        let struct_time = exec_time.elapsed().as_millis();
        let q2: Vec<(i32, i32)> = q2.par_iter()
            .flat_map(|p| (p.time.start..p.time.end + 1).into_par_iter().map(move |i| (p.nid, i)))
            .collect();

        log(format!("q2(x,t){:?}", &q2), 5, debug_flag);
        log(format!("{:?},q2 {:?},{:?},{:?}, {:?}", &num_thred_, data_name, struct_time, exec_time.elapsed().as_millis(), &q2.len()), 1, debug_flag);
    }


    if query_list.contains(&3) {
        //Q3 MATCH (x: Person { risk = 'low ' AND time = '1'}) ON contact_tracing
        let exec_time = Instant::now();
        let q3: Vec<&Node> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::low))
            .collect();
        let struct_time = exec_time.elapsed().as_millis();
        let q3: Vec<(i32, i32)> = q3.into_par_iter()
            .flat_map(|p|  (p.time.start..p.time.end+1).into_par_iter().map(move |i| (p.nid,i)))
            .filter(|(pid, time)| *time == 1)
            .collect();
        log(format!("q3(x,t){:?}", &q3), 5, debug_flag);
        //log(format!("t, #q3 {:?}, {:?}", exec_time.elapsed().as_millis(), &q3.len()), 1, debug_flag);
        log(format!("{:?},q3 {:?},{:?},{:?}, {:?}", &num_thred_, data_name,struct_time,exec_time.elapsed().as_millis(), &q3.len()), 1, debug_flag);
    }

    if query_list.contains(&4) {
        //Q4 MATCH (x: Person { risk = 'low ' AND time < '10'}) ON contact_tracing
        let exec_time = Instant::now();
        let q4: Vec<&Node> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::low)).collect();
        let struct_time = exec_time.elapsed().as_millis();
        let q4: Vec<(i32, i32)> = q4.into_par_iter()
            .flat_map(|p|  (p.time.start..p.time.end+1).into_par_iter().map(move |i| (p.nid,i)))
            .filter(|(pid, time)| *time < 10)
            .collect();
        log(format!("q4(x,t){:?}", &q4), 5, debug_flag);
        //log(format!("t, #q4 {:?}, {:?}", exec_time.elapsed().as_millis(), &q4.len()), 1, debug_flag);
        log(format!("{:?},q4 {:?},{:?},{:?}, {:?}", &num_thred_, data_name,0,exec_time.elapsed().as_millis(), &q4.len()), 1, debug_flag);
    }

    if query_list.contains(&5) {
        /* Q5 MATCH (x: Person { risk = 'low '}) -[z: meets ]->(y: Person { risk = 'high '}) */
        let exec_time = Instant::now();
        let x: Vec<(i32, Node)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::low)).map(|p| (p.nid, p.clone())).collect();
        let y: Vec<(i32, Node)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.clone())).collect();
        let mut zp: Vec<(i32, Edge)> = edges.par_iter().filter(|e| matches!(e.label,Label::meets)).map(|e| (e.src, e.clone())).collect();



        zp = hash_join(&zp, &x).into_par_iter()
            .filter_map(|(e, _, n)| match e.time.overlap(&n.time) {
                true => Some(Edge {
                    eid: e.eid,
                    src: e.src,
                    dst: e.dst,
                    label: e.label,
                    prop1: e.prop1,
                    time: e.time.intersect(&n.time)
                }),
                false => None
            }).map(|e| (e.dst, e))
            .collect();
        let q5: Vec<Edge> = hash_join(&zp, &y).into_par_iter()
            .filter_map(|(e, _, n)| match e.time.overlap(&n.time) {
                true => Some(Edge {
                    eid: e.eid,
                    src: e.src,
                    dst: e.dst,
                    label: e.label,
                    prop1: e.prop1,
                    time: e.time.intersect(&n.time)
                }),
                false => None
            }).collect();
        let struct_time = exec_time.elapsed().as_millis();

        let q5: Vec<Vec<(i32, i32)>> = q5.par_iter()
            .flat_map(|e| (e.time.zip_vec(&vec![e.src, e.eid, e.dst])))
            //.map(|e|(e.time.zip(e.src),(e.time.zip(e.eid)),(e.time.zip(e.dst))))
            .collect();
        log(format!("q5[(x,t),(z,t),(y,t)]{:?}", &q5), 5, debug_flag);
        log(format!("{:?},q5 {:?},{:?},{:?}, {:?}", &num_thred_, data_name,struct_time,exec_time.elapsed().as_millis(), &q5.len()), 1, debug_flag);

        //log(format!("st, t, #q5:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q5.len()), 1, debug_flag);
    }
    // if query_list.contains(&5) {
    //     /* Q5 MATCH (x: Person { risk = 'low '}) -[z: meets ]->(y: Person { risk = 'high '}) */
    //     let exec_time = Instant::now();
    //     let x: Vec<(i32, Node)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::low)).map(|p| (p.nid, p.clone())).collect();
    //     let y: Vec<(i32, Node)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.clone())).collect();
    //     let mut zp: Vec<(i32, Edge)> = edges.par_iter().filter(|e| matches!(e.label,Label::meets)).map(|e| (e.src, e.clone())).collect();
    //
    //     zp = hash_join_2(&zp, &x).into_par_iter()
    //         .filter_map(|(e, _, n)| match e.time.overlap(&n.time) {
    //             true => Some(Edge {
    //                 eid: e.eid,
    //                 src: e.src,
    //                 dst: e.dst,
    //                 label: e.label,
    //                 prop1: e.prop1,
    //                 time: e.time.intersect(&n.time)
    //             }),
    //             false => None
    //         }).map(|e| (e.dst, e)).collect();
    //     let q5: Vec<Edge> = hash_join_2(&zp, &y).into_par_iter()
    //         .filter_map(|(e, _, n)| match e.time.overlap(&n.time) {
    //             true => Some(Edge {
    //                 eid: e.eid,
    //                 src: e.src,
    //                 dst: e.dst,
    //                 label: e.label,
    //                 prop1: e.prop1,
    //                 time: e.time.intersect(&n.time)
    //             }),
    //             false => None
    //         }).collect();
    //     let struct_time = exec_time.elapsed().as_millis();
    //
    //     let q5: Vec<Vec<(i32, i32)>> = q5.par_iter()
    //         .flat_map(|e| (e.time.zip_vec(&vec![e.src, e.eid, e.dst])))
    //         //.map(|e|(e.time.zip(e.src),(e.time.zip(e.eid)),(e.time.zip(e.dst))))
    //         .collect();
    //     log(format!("q5[(x,t),(z,t),(y,t)]{:?}", &q5), 5, debug_flag);
    //     log(format!("{:?},q5-p {:?},{:?},{:?}, {:?}", &num_thred_, data_name,struct_time,exec_time.elapsed().as_millis(), &q5.len()), 1, debug_flag);
    //
    //     //log(format!("st, t, #q5:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q5.len()), 1, debug_flag);
    // }


    // Q6 MATCH (x: Person { test = 'pos '}) - / PREV /-(y: Person ) ON contact_tracing
    if query_list.contains(&6) {
        let exec_time = Instant::now();
        let x: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|x| (x.nid, x.time.clone())).collect()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let nn: Vec<(i32, Interval)> = nodes.par_iter().map(|p| (p.nid, p.time.clone())).collect();
        let q6: Vec<(Interval, i32, Interval)> = hash_join(&nn, &x);
        let struct_time = exec_time.elapsed().as_millis();
        let q6: Vec<((i32, i32), (i32, i32))> = q6
            .into_par_iter()
            .filter_map(|(yt, x, xt)|
                match yt.prev(&xt) {
                    None => { None }
                    //Some(yt) => Some((xt.toPoints().into_iter().cartesian_product(yt.toPoints()).par_bridge().filter(|(xt, yt)| *xt - 1 == *yt).map(move |(xt, yt)| ((x, xt), (x, yt)))))
                    Some(yt) => Some(yt.zip_vec(&xt.toPoints()).into_par_iter().flat_map(|x|x).filter(|(xt, yt)| *xt - 1 == *yt).map( move|(xt, yt)| ((x, xt), (x, yt))))
                   //x.zip_vec(y) -> (y,x)

                })
            .flat_map(|x| x)
            .collect();
        log(format!("q6[(x,t),(y,t)]{:?}", &q6), 5, debug_flag);
        //log(format!("st, t, #q6:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q6.len()), 1, debug_flag);
        log(format!("{:?},q6 {:?},{:?},{:?}, {:?}", &num_thred_, data_name,struct_time,exec_time.elapsed().as_millis(), &q6.len()), 1, debug_flag);

    }
    if query_list.contains(&6) {
        let exec_time = Instant::now();
        let x: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|x| (x.nid, x.time.clone())).collect()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let nn: Vec<(i32, Interval)> = nodes.par_iter().map(|p| (p.nid, p.time.clone())).collect();
        let q6: Vec<(Interval, i32, Interval)> = hash_join_2(&nn, &x);
        let struct_time = exec_time.elapsed().as_millis();
        let q6: Vec<((i32, i32), (i32, i32))> = q6
            .into_par_iter()
            .filter_map(|(yt, x, xt)|
                match yt.prev(&xt) {
                    None => { None }
                    //Some(yt) => Some((xt.toPoints().into_iter().cartesian_product(yt.toPoints()).par_bridge().filter(|(xt, yt)| *xt - 1 == *yt).map(move |(xt, yt)| ((x, xt), (x, yt)))))
                    Some(yt) => Some(yt.zip_vec(&xt.toPoints()).into_par_iter().flat_map(|x|x).filter(|(xt, yt)| *xt - 1 == *yt).map( move|(xt, yt)| ((x, xt), (x, yt))))
                    //x.zip_vec(y) -> (y,x)

                })
            .flat_map(|x| x)
            .collect();
        log(format!("q6-p[(x,t),(y,t)]{:?}", &q6), 5, debug_flag);
        //log(format!("st, t, #q6:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q6.len()), 1, debug_flag);
        log(format!("{:?},q6-p {:?},{:?},{:?}, {:?}", &num_thred_, data_name,struct_time,exec_time.elapsed().as_millis(), &q6.len()), 1, debug_flag);

    }

    if query_list.contains(&7) {

        // Q7 MATCH (x: Person { test = 'pos '}) -     / PREV /-() -[: visits ]->(z:Class )
        let exec_time = Instant::now();
        let x: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let z: Vec<(i32, (i32, Interval))> = edges.par_iter().filter(|e| matches!(e.label,Label::visits)).map(|e| (e.src, (e.dst, e.time.clone()))).collect();
        let q7: Vec<((i32, Interval), i32, Interval)> = hash_join(&z, &x);
        let struct_time = exec_time.elapsed().as_millis();
        let q7: Vec<((i32, i32), (i32, i32))> = q7.into_par_iter()
            .filter_map(|((z, zt), x, xt)|
                match zt.prev(&xt) {
                    None => None,
                    //Some(zt) => Some((xt.toPoints().into_iter().cartesian_product(zt.toPoints()).filter(|(xt, zt)| *xt - 1 == *zt).map(move |(xt, zt)| ((x, xt), (z, zt)))))
                    //Some(zt) => Some((xt.toPoints().into_iter().cartesian_product(zt.toPoints()).filter(|(xt, zt)| *xt - 1 == *zt).map(move |(xt, zt)| ((x, xt), (z, zt)))))
                    Some(zt) => Some(zt.zip_vec(&xt.toPoints()).into_par_iter().flat_map(|x|x).filter(|(xt, zt)| *xt - 1 == *zt).map( move|(xt, zt)| ((x, xt), (z, zt))))

                })
            .flat_map(|x| x)
            .collect();
        log(format!("q7[(x,t),(z,t)]{:?}", &q7), 5, debug_flag);
        log(format!("{:?},q7,{:?},{:?}, {:?},{:?}",num_thred_, data_name,struct_time,exec_time.elapsed().as_millis(), &q7.len()), 1, debug_flag);
    }

    if query_list.contains(&7) {

        // Q7 MATCH (x: Person { test = 'pos '}) -     / PREV /-() -[: visits ]->(z:Class )
        let exec_time = Instant::now();
        let x: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let z: Vec<(i32, (i32, Interval))> = edges.par_iter().filter(|e| matches!(e.label,Label::visits)).map(|e| (e.src, (e.dst, e.time.clone()))).collect();
        let q7: Vec<((i32, Interval), i32, Interval)> = hash_join_2(&z, &x);
        let struct_time = exec_time.elapsed().as_millis();
        let q7: Vec<((i32, i32), (i32, i32))> = q7.into_par_iter()
            .filter_map(|((z, zt), x, xt)|
                match zt.prev(&xt) {
                    None => None,
                    //Some(zt) => Some((xt.toPoints().into_iter().cartesian_product(zt.toPoints()).filter(|(xt, zt)| *xt - 1 == *zt).map(move |(xt, zt)| ((x, xt), (z, zt)))))
                    //Some(zt) => Some((xt.toPoints().into_iter().cartesian_product(zt.toPoints()).filter(|(xt, zt)| *xt - 1 == *zt).map(move |(xt, zt)| ((x, xt), (z, zt)))))
                    Some(zt) => Some(zt.zip_vec(&xt.toPoints()).into_par_iter().flat_map(|x|x).filter(|(xt, zt)| *xt - 1 == *zt).map( move|(xt, zt)| ((x, xt), (z, zt))))

                })
            .flat_map(|x| x)
            .collect();
        log(format!("q7[(x,t),(z,t)]{:?}", &q7), 5, debug_flag);
        log(format!("{:?},q7-p,{:?},{:?},{:?},{:?}",num_thred_,data_name,struct_time,exec_time.elapsed().as_millis(), &q7.len()), 1, debug_flag);
    }
    if query_list.contains(&8) {

        //Q8  MATCH (x: Person { test = 'pos '}) - / PREV* /-() -[: visits ]->(z:Class )
        let exec_time = Instant::now();
        let x: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let z: Vec<(i32, (i32, Interval))> = edges.par_iter().filter(|e| matches!(e.label,Label::visits)).map(|e| (e.src, (e.dst, e.time.clone()))).collect();
        let q8 = hash_join(&z, &x);
        let struct_time = exec_time.elapsed().as_millis();
        let q8: Vec<((i32, i32), (i32, i32))> = q8.into_par_iter()
            .filter(|((z, zt), x, xt)| zt.isPrev(&xt))
            .map(|((z, zt), x, xt)| (zt.zip_vec(&xt.toPoints()).into_par_iter().flat_map(|x|x).filter(|(xt, zt)| *zt  <= *xt).map(move |(xt, zt)| ((x, xt), (z, zt)))))
            .flat_map(|x| x)
            .collect();
        log(format!("q8[(x,t),(z,t)]{:?}", &q8), 5, debug_flag);
        log(format!("{:?},q8 {:?},{:?},{:?}, {:?}", &num_thred_, data_name,struct_time,exec_time.elapsed().as_millis(), &q8.len()), 1, debug_flag);

    }

    if query_list.contains(&8) {

        //Q8  MATCH (x: Person { test = 'pos '}) - / PREV* /-() -[: visits ]->(z:Class )
        let exec_time = Instant::now();
        let x: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let z: Vec<(i32, (i32, Interval))> = edges.par_iter().filter(|e| matches!(e.label,Label::visits)).map(|e| (e.src, (e.dst, e.time.clone()))).collect();
        let q8 = hash_join_2(&z, &x);
        let struct_time = exec_time.elapsed().as_millis();
        let q8: Vec<((i32, i32), (i32, i32))> = q8.into_par_iter()
            .filter(|((z, zt), x, xt)| zt.isPrev(&xt))
            .map(|((z, zt), x, xt)| (zt.zip_vec(&xt.toPoints()).into_par_iter().flat_map(|x|x).filter(|(xt, zt)| *zt  <= *xt).map(move |(xt, zt)| ((x, xt), (z, zt)))))
            .flat_map(|x| x)
            .collect();
        log(format!("q8[(x,t),(z,t)]{:?}", &q8), 5, debug_flag);
        log(format!("{:?},q8-p {:?},{:?},{:?}, {:?}", &num_thred_, data_name,struct_time,exec_time.elapsed().as_millis(), &q8.len()), 1, debug_flag);
    }


    if query_list.contains(&9) {


        // // MATCH (x: Person { risk = 'high '}) - / FWD /: meets / FWD / NEXT */ -({ test = 'pos '})
        //
        let exec_time = Instant::now();
        let x: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.time.clone())).collect()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let y: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let mut zp: Vec<(i32, Edge)> = edges.par_iter().filter(|e| matches!(e.label,Label::meets)).map(|e| (e.src, e.clone())).collect();

        let zpp: Vec<(i32, (i32, Interval))> = hash_join(&zp, &x).into_par_iter()
            .filter_map(|(e, _, n)| match e.time.overlap(&n) {
                true => Some((e.dst, (e.src, e.time.intersect(&n)))),
                false => None
            }).collect();
        let zppp: Vec<((i32, Interval), (i32, Interval))> = hash_join(&zpp, &y).into_par_iter().map(|((x, xt), _, yt)| ((x, xt), (x, yt))).collect();
        log(format!("zppp[(x,t)]{:?}", &zppp), 5, debug_flag);
        let struct_time = exec_time.elapsed().as_millis();
        let q9: Vec<(i32, i32)> = zppp.into_par_iter()
            .filter(|((x, xt), (_, yt))| yt.isNext(&xt))
            .map(|((x, xt), (_, yt))| (yt.zip_two_points(&xt.toPoints()).into_par_iter().filter(|(xt, yt)| *yt - *xt >= 0)).map(move |(xt, yt)| ((x, xt))))
            .flat_map(|x| x)
            .collect();
        log(format!("q9[(x,t)]{:?}", &q9), 5, debug_flag);
        //log(format!("st, t, #q9:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q9.len()), 1, debug_flag);
        log(format!("{:?},q9 {:?},{:?},{:?}, {:?}", &num_thred_, data_name,struct_time,exec_time.elapsed().as_millis(), &q9.len()), 1, debug_flag);

    }

    if query_list.contains(&9) {


        // // MATCH (x: Person { risk = 'high '}) - / FWD /: meets / FWD / NEXT */ -({ test = 'pos '})
        //
        let exec_time = Instant::now();
        let x: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.time.clone())).collect()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let y: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let mut zp: Vec<(i32, Edge)> = edges.par_iter().filter(|e| matches!(e.label,Label::meets)).map(|e| (e.src, e.clone())).collect();

        let zpp: Vec<(i32, (i32, Interval))> = hash_join_2(&zp, &x).into_par_iter()
            .filter_map(|(e, _, n)| match e.time.overlap(&n) {
                true => Some((e.dst, (e.src, e.time.intersect(&n)))),
                false => None
            })
            .collect();
        let zppp: Vec<((i32, Interval), (i32, Interval))> = hash_join_2(&zpp, &y).into_par_iter().map(|((x, xt), _, yt)| ((x, xt), (x, yt))).collect();
        log(format!("zppp[(x,t)]{:?}", &zppp), 5, debug_flag);
        let struct_time = exec_time.elapsed().as_millis();
        let q9: Vec<(i32, i32)> = zppp.into_par_iter()
            .filter(|((x, xt), (_, yt))| yt.isNext(&xt))
            .map(|((x, xt), (_, yt))| (yt.zip_two_points(&xt.toPoints()).into_par_iter().filter(|(xt, yt)| *yt - *xt >= 0)).map(move |(xt, yt)| ((x, xt))))
            .flat_map(|x| x)
            .collect();
        log(format!("q9[(x,t)]{:?}", &q9), 5, debug_flag);
        //log(format!("st, t, #q9:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q9.len()), 1, debug_flag);
        log(format!("{:?},q9-p {:?},{:?},{:?}, {:?}", &num_thred_, data_name,struct_time,exec_time.elapsed().as_millis(), &q9.len()), 1, debug_flag);

    }

    if query_list.contains(&10) {
        //     //Q10 MATCH (x:Person {risk = 'high'})- /FWD/:meets/FWD/PREV[0,12]-({test = 'pos'}) ON contact_tracing
        //
        let exec_time = Instant::now();
        let x: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.time.clone())).collect();
        let y: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect();
        let p: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person)).map(|p| (p.nid, p.time.clone())).collect();
        let y_pos: Vec<(i32, (Interval, Interval))> = hash_join(&y, &p).into_par_iter()
            .filter_map(|(ypt, y, yt)| match yt.isBefore(&ypt) {
                true => Some((y, (yt,ypt))),
                false => None
            }).collect(); // every person tested positive and their interval

        let mut zp: Vec<(i32, Edge)> = edges.par_iter()
            .filter(|e|  matches!(e.label,Label::meets) )
            .map(|e| (e.dst, e.clone())).collect(); //all edges

        // all high risk and their edges
        let zpp: Vec<(i32, (Interval, Interval))> = hash_join(&zp, &y_pos).into_par_iter().filter_map(|(e, _, (yt,ytp))|
            match ytp.isNext(&e.time)   {
                true => Some((e.src,(e.time,  ytp))),
                false => None
            }).collect();


        let meets: Vec<(i32, Interval, Interval)> = hash_join(&zpp, &x).into_par_iter()
            .map(|((etime,ytp),x,xt)|(x,etime,ytp))
            .collect();
        //log(format!("zppp[(x,t)]{:?}", &zppp), 5, debug_flag);
        let mut zppp = meets;

        let struct_time = exec_time.elapsed().as_millis();
        let q10:Vec<(i32, i32)> = zppp.into_par_iter()
            //.map(|((x, xt, yt))| (xt.toPoints().into_iter().cartesian_product(yt.toPoints()).filter(|(xt, yt)| *yt - *xt <= qt.parse().unwrap())).map(move |(xt, yt)| (x, xt)))
            .map(|((x, xt, yt))| (yt.zip_two_points(&xt.toPoints()).into_par_iter().filter(|(xt, yt)| *yt - *xt <= qt.parse().unwrap())).map(move |(xt, yt)| (x, xt)))
            .flat_map(|x| x)
            .collect();
        log(format!("10[(x,t)]{:?}", &q10), 5, debug_flag);
        //log(format!("st, t, #q10:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q10.len()), 1, debug_flag);
        log(format!("{:?},q10 {:?},{:?},{:?}, {:?}", &num_thred_, data_name,struct_time,exec_time.elapsed().as_millis(), &q10.len()), 1, debug_flag);

    }

    if query_list.contains(&10) {
        //     //Q10 MATCH (x:Person {risk = 'high'})- /FWD/:meets/FWD/PREV[0,12]-({test = 'pos'}) ON contact_tracing
        //
        let exec_time = Instant::now();
        let x: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.time.clone())).collect();
        let y: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect();
        let p: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person)).map(|p| (p.nid, p.time.clone())).collect();
        let y_pos: Vec<(i32, (Interval, Interval))> = hash_join_2(&y, &p).into_par_iter()
            .filter_map(|(ypt, y, yt)| match yt.isBefore(&ypt) {
                true => Some((y, (yt,ypt))),
                false => None
            }).collect(); // every person tested positive and their interval

        let mut zp: Vec<(i32, Edge)> = edges.par_iter()
            .filter(|e|  matches!(e.label,Label::meets) )
            .map(|e| (e.dst, e.clone())).collect(); //all edges

        // all high risk and their edges
        let zpp: Vec<(i32, (Interval, Interval))> = hash_join_2(&zp, &y_pos).into_par_iter().filter_map(|(e, _, (yt,ytp))|
            match ytp.isNext(&e.time)   {
                true => Some((e.src,(e.time,  ytp))),
                false => None
            }).collect();


        let meets: Vec<(i32, Interval, Interval)> = hash_join_2(&zpp, &x).into_par_iter()
            .map(|((etime,ytp),x,xt)|(x,etime,ytp))
            .collect();
        //log(format!("zppp[(x,t)]{:?}", &zppp), 5, debug_flag);
        let mut zppp = meets;

        let struct_time = exec_time.elapsed().as_millis();
        let q10:Vec<(i32, i32)> = zppp.into_par_iter()
            //.map(|((x, xt, yt))| (xt.toPoints().into_iter().cartesian_product(yt.toPoints()).filter(|(xt, yt)| *yt - *xt <= qt.parse().unwrap())).map(move |(xt, yt)| (x, xt)))
            .map(|((x, xt, yt))| (yt.zip_two_points(&xt.toPoints()).into_par_iter().filter(|(xt, yt)| *yt - *xt <= qtt)).map(move |(xt, yt)| (x, xt)))
            .flat_map(|x| x)
            .collect();
        log(format!("10[(x,t)]{:?}", &q10), 5, debug_flag);
        //log(format!("st, t, #q10:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q10.len()), 1, debug_flag);
        log(format!("{:?},q10-p {:?},{:?},{:?}, {:?}", &num_thred_, data_name,struct_time,exec_time.elapsed().as_millis(), &q10.len()), 1, debug_flag);

    }

    if query_list.contains(&11) {
        //Q11 MATCH (x:Person {risk = 'high'})-// /FWD/:visits/FWD/:Room/BWD/:visits/// BWD/NEXT[0,12]/-({test = 'pos'})
        let exec_time = Instant::now();
        let x: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.time.clone())).collect();
        let y: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect();
        let p: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person)).map(|p| (p.nid, p.time.clone())).collect();
        let y_pos: Vec<(i32, (Interval, Interval))> = hash_join(&y, &p).into_par_iter()
            .filter_map(|(ypt, y, yt)| match yt.isBefore(&ypt) {
                true => Some((y, (yt,ypt))),
                false => None
            }).collect(); // every person tested positive and their interval


        let mut visit_edges:Vec<(i32,Edge)> = edges.par_iter().filter(|e|  matches!(e.label,Label::visits) ).map(|e| (e.src, e.clone())).collect(); //all high risk who ride a bud
        let yppp: Vec<(i32, (i32, Interval, Interval))> = hash_join(&visit_edges, &y_pos).into_par_iter()
            .filter_map(|(e, _, (n,np))|  match e.time.isPrev(&np) {
                true => Some((e.dst, (e.src, e.time,np))),
                false => None
            }).collect();

        let xpp: Vec<(i32, (i32, Interval))> = hash_join(&visit_edges, &x).into_par_iter()
            .filter_map(|(e, _, n)|  match e.time.overlap(&n) {
                true => Some((e.dst, (e.src, e.time))),
                false => None
            })

            .collect();

        let mut room:Vec<(i32,Interval,Interval)> = hash_join(&xpp, &yppp).into_par_iter().filter_map(|((x,xt), _, (y,yt,ytp))| match xt.overlap(&yt) {
            true => Some(((x,xt.intersect(&yt),ytp))),
            false => None
        })
            .collect();

        let zppp= room;
        //zppp.dedup();

        let struct_time = exec_time.elapsed().as_millis();
        let q11:Vec<(i32,i32)> = zppp.into_par_iter()
            //.map(|((x, xt, yt))| (xt.toPoints().into_iter().cartesian_product(yt.toPoints()).filter(|(xt, yt)| *yt - *xt <= qt.parse().unwrap())).map(move |(xt, yt)| (x, xt)))
            .map(|((x, xt, yt))| (yt.zip_two_points(&xt.toPoints()).into_par_iter().filter(|(xt, yt)| *yt - *xt <= qtt)).map(move |(xt, yt)| (x, xt)))

            .flat_map(|x| x)
            .collect();
        log(format!("q11[(x,t)]{:?}", &q11), 5, debug_flag);
        //log(format!("st, t, #q11:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q11.len()), 1, debug_flag);
        log(format!("{:?},q11 {:?},{:?},{:?}, {:?}", &num_thred_, data_name,struct_time,exec_time.elapsed().as_millis(), &q11.len()), 1, debug_flag);
    }

    if query_list.contains(&11) {
        //Q11 MATCH (x:Person {risk = 'high'})-// /FWD/:visits/FWD/:Room/BWD/:visits/// BWD/NEXT[0,12]/-({test = 'pos'})
        let exec_time = Instant::now();
        let x: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.time.clone())).collect();
        let y: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect();
        let p: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person)).map(|p| (p.nid, p.time.clone())).collect();
        let y_pos: Vec<(i32, (Interval, Interval))> = hash_join_2(&y, &p).into_par_iter()
            .filter_map(|(ypt, y, yt)| match yt.isBefore(&ypt) {
                true => Some((y, (yt,ypt))),
                false => None
            }).collect(); // every person tested positive and their interval


        let mut visit_edges:Vec<(i32,Edge)> = edges.par_iter().filter(|e|  matches!(e.label,Label::visits) ).map(|e| (e.src, e.clone())).collect(); //all high risk who ride a bud
        let yppp: Vec<(i32, (i32, Interval, Interval))> = hash_join_2(&visit_edges, &y_pos).into_par_iter()
            .filter_map(|(e, _, (n,np))|  match e.time.isPrev(&np) {
                true => Some((e.dst, (e.src, e.time,np))),
                false => None
            }).collect();

        let xpp: Vec<(i32, (i32, Interval))> = hash_join_2(&visit_edges, &x).into_par_iter()
            .filter_map(|(e, _, n)|  match e.time.overlap(&n) {
                true => Some((e.dst, (e.src, e.time))),
                false => None
            }).collect();

        let mut room:Vec<(i32,Interval,Interval)> = hash_join_2(&xpp, &yppp).into_par_iter().filter_map(|((x,xt), _, (y,yt,ytp))| match xt.overlap(&yt) {
            true => Some(((x,xt.intersect(&yt),ytp))),
            false => None
        })
            .collect();

        let zppp= room;
        //zppp.dedup();

        let struct_time = exec_time.elapsed().as_millis();
        let q11:Vec<(i32,i32)> = zppp.into_par_iter()
            //.map(|((x, xt, yt))| (xt.toPoints().into_iter().cartesian_product(yt.toPoints()).filter(|(xt, yt)| *yt - *xt <= qt.parse().unwrap())).map(move |(xt, yt)| (x, xt)))
            .map(|((x, xt, yt))| (yt.zip_two_points(&xt.toPoints()).into_par_iter().filter(|(xt, yt)| *yt - *xt <= qtt)).map(move |(xt, yt)| (x, xt)))
            .flat_map(|x| x)
            .collect();
        log(format!("q11[(x,t)]{:?}", &q11), 5, debug_flag);
        //log(format!("st, t, #q11:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q11.len()), 1, debug_flag);
        log(format!("{:?},q11-p {:?},{:?},{:?}, {:?}", &num_thred_, data_name,struct_time,exec_time.elapsed().as_millis(), &q11.len()), 1, debug_flag);
    }

    if query_list.contains(&12) {

        //MATCH (x:Person {risk = 'high'})- /(FWD/:meets/FWD/NEXT[0,12]) + (FWD/:visits/FWD/:Room/BWD/:visits/ BWD/NEXT[0,12])/-({test = 'pos'})

        let exec_time = Instant::now();
        let x: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.time.clone())).collect();
        let y: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect();
        let p: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person)).map(|p| (p.nid, p.time.clone())).collect();
        let y_pos: Vec<(i32, (Interval, Interval))> = hash_join(&y, &p).into_par_iter()
            .filter_map(|(ypt, y, yt)| match yt.isBefore(&ypt) {
                true => Some((y, (yt,ypt))),
                false => None
            }).collect(); // every person tested positive and their interval

        let mut zp:Vec<(i32,Edge)> = edges.par_iter()
            .filter(|e|  matches!(e.label,Label::meets) )
            .map(|e| (e.dst, e.clone())).collect(); //all edges

        // all high risk and their edges
        let zpp:Vec<(i32,(Interval,Interval))> = hash_join(&zp, &y_pos).into_par_iter().filter_map(|(e, _, (yt,ytp))|
            match ytp.isNext(&e.time)   {
                true => Some((e.src,(e.time,  ytp))),
                false => None
            }).collect();


        let meets:Vec<(i32,Interval,Interval)> = hash_join(&zpp, &x).into_par_iter()
            .map(|((etime,ytp),x,xt)|(x,etime,ytp))
            .collect();

        //log(format!("zppp[(x,t)]{:?}", &zppp), 5, debug_flag);



        let mut visits_edges:Vec<(i32,Edge)> = edges.par_iter().filter(|e|  matches!(e.label,Label::visits) ).map(|e| (e.src, e.clone())).collect(); //all high risk who ride a bud
        let yppp: Vec<(i32, (i32, Interval, Interval))> = hash_join(&visits_edges, &y_pos).into_par_iter()
            .filter_map(|(e, _, (n,np))|  match e.time.isPrev(&np) {
                true => Some((e.dst, (e.src, e.time,np))),
                false => None
            }).collect();

        let xpp: Vec<(i32, (i32, Interval))> = hash_join(&visits_edges, &x).into_par_iter()
            .filter_map(|(e, _, n)|  match e.time.overlap(&n) {
                true => Some((e.dst, (e.src, e.time))),
                false => None
            }) .collect();

        let mut room: Vec<(i32, Interval, Interval)> = hash_join(&xpp, &yppp).into_par_iter().filter_map(|((x,xt), _, (y,yt,ytp))| match xt.overlap(&yt) {
            true => Some(((x,xt.intersect(&yt),ytp))),
            false => None
        })
            .collect();

        let mut zppp: Vec<(i32, Interval, Interval)> = meets;
        zppp.append(&mut room);
        //zppp.dedup();

        let struct_time = exec_time.elapsed().as_millis();
        let q12:Vec<(i32,i32)> = zppp.into_par_iter()
            //.map(|((x, xt, yt))| (xt.toPoints().into_iter().cartesian_product(yt.toPoints()).filter(|(xt, yt)| *yt - *xt <= qt.parse().unwrap())).map(move |(xt, yt)| (x, xt)))
            .map(|((x, xt, yt))| (yt.zip_two_points(&xt.toPoints()).into_par_iter().filter(|(xt, yt)| *yt - *xt <= qtt)).map(move |(xt, yt)| (x, xt)))
            .flat_map(|x| x)
            .collect();
        log(format!("q12[(x,t)]{:?}", &q12), 5, debug_flag);
        //log(format!("st, t, #q11:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q11.len()), 1, debug_flag);
        log(format!("{:?},q12 {:?},{:?},{:?}, {:?}", &num_thred_, data_name,struct_time,exec_time.elapsed().as_millis(), &q12.len()), 1, debug_flag);

    }

    if query_list.contains(&12) {

        //MATCH (x:Person {risk = 'high'})- /(FWD/:meets/FWD/NEXT[0,12]) + (FWD/:visits/FWD/:Room/BWD/:visits/ BWD/NEXT[0,12])/-({test = 'pos'})

        let exec_time = Instant::now();
        let x: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.time.clone())).collect();
        let y: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect();
        let p: Vec<(i32, Interval)> = nodes.par_iter().filter(|p| matches!(p.label,Label::person)).map(|p| (p.nid, p.time.clone())).collect();
        let y_pos: Vec<(i32, (Interval, Interval))> = hash_join_2(&y, &p).into_par_iter()
            .filter_map(|(ypt, y, yt)| match yt.isBefore(&ypt) {
                true => Some((y, (yt,ypt))),
                false => None
            }).collect(); // every person tested positive and their interval

        let mut zp:Vec<(i32,Edge)> = edges.par_iter()
            .filter(|e|  matches!(e.label,Label::meets) )
            .map(|e| (e.dst, e.clone())).collect(); //all edges

        // all high risk and their edges
        let zpp:Vec<(i32,(Interval,Interval))> = hash_join_2(&zp, &y_pos).into_par_iter().filter_map(|(e, _, (yt,ytp))|
            match ytp.isNext(&e.time)   {
                true => Some((e.src,(e.time,  ytp))),
                false => None
            }).collect();


        let meets:Vec<(i32,Interval,Interval)> = hash_join_2(&zpp, &x).into_par_iter()
            .map(|((etime,ytp),x,xt)|(x,etime,ytp))
            .collect();
        //log(format!("zppp[(x,t)]{:?}", &zppp), 5, debug_flag);



        let mut visits_edges:Vec<(i32,Edge)> = edges.par_iter().filter(|e|  matches!(e.label,Label::visits) ).map(|e| (e.src, e.clone())).collect(); //all high risk who ride a bud
        let yppp: Vec<(i32, (i32, Interval, Interval))> = hash_join_2(&visits_edges, &y_pos).into_par_iter()
            .filter_map(|(e, _, (n,np))|  match e.time.isPrev(&np) {
                true => Some((e.dst, (e.src, e.time,np))),
                false => None
            }).collect();

        let xpp: Vec<(i32, (i32, Interval))> = hash_join_2(&visits_edges, &x).into_par_iter()
            .filter_map(|(e, _, n)|  match e.time.overlap(&n) {
                true => Some((e.dst, (e.src, e.time))),
                false => None
            }) .collect();

        let mut room: Vec<(i32, Interval, Interval)> = hash_join_2(&xpp, &yppp).into_par_iter().filter_map(|((x,xt), _, (y,yt,ytp))| match xt.overlap(&yt) {
            true => Some(((x,xt.intersect(&yt),ytp))),
            false => None
        })
            .collect();

        let mut zppp: Vec<(i32, Interval, Interval)> = meets;
        zppp.append(&mut room);
        //zppp.dedup();

        let struct_time = exec_time.elapsed().as_millis();
        let q12:Vec<(i32,i32)> = zppp.into_par_iter()
            //.map(|((x, xt, yt))| (xt.toPoints().into_iter().cartesian_product(yt.toPoints()).filter(|(xt, yt)| *yt - *xt <= qt.parse().unwrap())).map(move |(xt, yt)| (x, xt)))
            .map(|((x, xt, yt))| (yt.zip_two_points(&xt.toPoints()).into_par_iter().filter(|(xt, yt)| *yt - *xt <= qtt)).map(move |(xt, yt)| (x, xt)))
            .flat_map(|x| x)
            .collect();
        log(format!("q12[(x,t)]{:?}", &q12), 5, debug_flag);
        //log(format!("st, t, #q11:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q11.len()), 1, debug_flag);
        log(format!("{:?},q12-p {:?},{:?},{:?}, {:?}", &num_thred_, data_name,struct_time,exec_time.elapsed().as_millis(), &q12.len()), 1, debug_flag);
    }
}


/*
"\"contact-big\",q1 ,7,934, 4961295"
"\"contact-big\",q2 ,3,848, 4763237"
"\"contact-big\",q3 ,3,302, 96008"
"\"contact-big\",q4 ,0,388, 864062"
"\"contact-big\",q5 ,52042,61481, 21324623"
"\"contact-big\",q6 ,20,790, 237009"
"\"contact-big\",q7,1759,2082, 85842"
"\"contact-big\",q8 ,336,1157, 2110332"
"\"contact-big\",q9 ,27263,33131, 30459794"
"\"contact-big\",q10 ,18355,18377, 198848"
"\"contact-big\",q11 ,1005,1649, 5795769"
"\"contact-big\",q12 ,7511,8104, 5994617"


 */
