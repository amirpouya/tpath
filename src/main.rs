extern crate tpath;
use itertools::Itertools;

use tpath::interval;
use tpath::interval::Interval;
use tpath::node::Node;
use tpath::edge::{Edge};
use std::collections::HashMap;
use std::ptr::null;
use tpath::join::hash_join;
use tpath::label::Label;
use std::time::Instant;


fn main() {



    let query_list = [1,2,3,4,5,6,7,8,9,10,11,12];
    fn log(input: String, level: usize, debug_flag: usize) {
        if debug_flag >= level {
            println!("{:?}", input);
        }
    }

    let mut config_addr = std::env::args().nth(1).unwrap_or(("/Users/amir/Documents/Projects/rust/tpath/data/example").to_string()).parse::<String>().unwrap();
    let qt = std::env::args().nth(2).unwrap_or(("12").to_string()).parse::<String>().unwrap();


    let file_name = config_addr.clone();
    let debug_flag = 30;


    //read the config
    let data_name = file_name.split("/").last().unwrap().clone();

    //let n = Node::get_from_file("/Users/amir/Documents/Projects/rust/tpath/data/simp/node.csv");
    let nodes = Node::get_from_file(&(config_addr.clone()+"/node.csv"));

    log(format!("Nodes{:?}", &nodes), 10, debug_flag);

    //let e = Edge::get_from_file("/Users/amir/Documents/Projects/rust/tpath/data/simp/edge.csv");
     let edges = Edge::get_from_file(&(config_addr.clone()+"/edge.csv"));

    log(format!("Edges{:?}", &edges), 10, debug_flag);


    //query exec

    if query_list.contains(&1) {
        //Q1 MATCH (x: Person) ON contact_tracing
        let exec_time = Instant::now();
        let q1 = nodes.iter().filter(|p| matches!(p.label,Label::person)).collect_vec();
        let struct_time = exec_time.elapsed().as_micros();
        let q1 = q1.into_iter()
            .flat_map(|p| (p.time.zip(p.nid)))
            .collect_vec();

        log(format!("q1(x,t){:?}", &q1), 5, debug_flag);
        log(format!("{:?},q1 ,{:?},{:?}, {:?}", data_name,struct_time,exec_time.elapsed().as_micros(), &q1.len()), 1, debug_flag);
    }
    if query_list.contains(&2) {
        //Q2 MATCH (x: Person { risk = 'low '}) ON contact_tracing
        let exec_time = Instant::now();
        let q2 = nodes.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::low)).collect_vec();
        let struct_time = exec_time.elapsed().as_micros();
        let q2 = q2.iter()
            .flat_map(|p| (p.time.zip(p.nid))).collect_vec();

        log(format!("q2(x,t){:?}", &q2), 5, debug_flag);
        log(format!("{:?},q2 ,{:?},{:?}, {:?}", data_name,struct_time,exec_time.elapsed().as_micros(), &q2.len()), 1, debug_flag);
    }


    if query_list.contains(&3) {
        //Q3 MATCH (x: Person { risk = 'low ' AND time = '1'}) ON contact_tracing
        let exec_time = Instant::now();
        let q3 = nodes.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::low))
            .collect_vec();
        let struct_time = exec_time.elapsed().as_micros();
        let q3 = q3.into_iter()
        .flat_map(|p| (p.time.zip(p.nid)))
            .filter(|(pid, time)| *time == 1)
            .collect_vec();
        log(format!("q3(x,t){:?}", &q3), 5, debug_flag);
        //log(format!("t, #q3 {:?}, {:?}", exec_time.elapsed().as_micros(), &q3.len()), 1, debug_flag);
        log(format!("{:?},q3 ,{:?},{:?}, {:?}", data_name,struct_time,exec_time.elapsed().as_micros(), &q3.len()), 1, debug_flag);

    }

    if query_list.contains(&4) {
        //Q4 MATCH (x: Person { risk = 'low ' AND time < '10'}) ON contact_tracing
        let exec_time = Instant::now();
        let q4 = nodes.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::low)).collect_vec();
        let struct_time = exec_time.elapsed().as_micros();
        let q4 = q4.into_iter()
        .flat_map(|p| (p.time.zip(p.nid)))
            .filter(|(pid, time)| *time < 10)
            .collect_vec();
        log(format!("q4(x,t){:?}", &q4), 5, debug_flag);
        //log(format!("t, #q4 {:?}, {:?}", exec_time.elapsed().as_micros(), &q4.len()), 1, debug_flag);
        log(format!("{:?},q4 ,{:?},{:?}, {:?}", data_name,0,exec_time.elapsed().as_micros(), &q4.len()), 1, debug_flag);

    }

    if query_list.contains(&5) {
        /* Q5 MATCH (x: Person { risk = 'low '}) -[z: meets ]->(y: Person { risk = 'high '}) */
        let exec_time = Instant::now();
        let x = nodes.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::low)).map(|p| (p.nid, p.clone())).collect_vec();
        let y = nodes.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.clone())).collect_vec();
        let mut zp = edges.iter().filter(|e| matches!(e.label,Label::meets)).map(|e| (e.src, e.clone())).collect_vec();
        zp = hash_join(&zp, &x).into_iter()
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
            .collect_vec();
        let q5 = hash_join(&zp, &y).into_iter()
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
            }).collect_vec();
        let struct_time = exec_time.elapsed().as_micros();

        let q5 = q5.iter()
            .flat_map(|e| (e.time.zip_vec(&vec![e.src, e.eid, e.dst])))
            //.map(|e|(e.time.zip(e.src),(e.time.zip(e.eid)),(e.time.zip(e.dst))))
            .collect_vec();
        log(format!("q5[(x,t),(z,t),(y,t)]{:?}", &q5), 5, debug_flag);
        log(format!("{:?},q5 ,{:?},{:?}, {:?}", data_name,struct_time,exec_time.elapsed().as_micros(), &q5.len()), 1, debug_flag);

        //log(format!("st, t, #q5:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_micros(), &q5.len()), 1, debug_flag);
    }


    // Q6 MATCH (x: Person { test = 'pos '}) - / PREV /-(y: Person ) ON contact_tracing
    if query_list.contains(&6) {
        let exec_time = Instant::now();
        let x = nodes.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos))/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let q6 = hash_join(&nodes.iter().map(|p| (p.nid, p.time.clone())).collect_vec(), &x.map(|x| (x.nid, x.time.clone())).collect_vec());
        let struct_time = exec_time.elapsed().as_micros();
        let q6 = q6
            .into_iter()
            .filter_map(|(yt, x, xt)|
                match yt.prev(&xt) {
                    None => { None }
                    Some(yt) => Some((xt.toPoints().into_iter().cartesian_product(yt.toPoints()).filter(|(xt, yt)| *xt - 1 == *yt).map(move |(xt, yt)| ((x, xt), (x, yt)))))
                })
            .flat_map(|x| x)
            .collect_vec();
        log(format!("q6[(x,t),(y,t)]{:?}", &q6), 5, debug_flag);
        //log(format!("st, t, #q6:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_micros(), &q6.len()), 1, debug_flag);
        log(format!("{:?},q6 ,{:?},{:?}, {:?}", data_name,struct_time,exec_time.elapsed().as_micros(), &q6.len()), 1, debug_flag);

    }


    if query_list.contains(&7) {

        // Q7 MATCH (x: Person { test = 'pos '}) -     / PREV /-() -[: visits ]->(z:Class )
        let exec_time = Instant::now();
        let x = nodes.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let z = edges.iter().filter(|e| matches!(e.label,Label::visits)).map(|e| (e.src, (e.dst, e.time.clone()))).collect_vec();
        let q7 = hash_join(&z, &x);
        let struct_time = exec_time.elapsed().as_micros();
        let q7 = q7.into_iter()
            .filter_map(|((z, zt), x, xt)|
                match zt.prev(&xt) {
                    None => None,
                    Some(zt) => Some((xt.toPoints().into_iter().cartesian_product(zt.toPoints()).filter(|(xt, zt)| *xt - 1 == *zt).map(move |(xt, zt)| ((x, xt), (z, zt)))))
                })
            .flat_map(|x| x)
            .collect_vec();
        log(format!("q7[(x,t),(z,t)]{:?}", &q7), 5, debug_flag);
        log(format!("{:?},q7,{:?},{:?}, {:?}", data_name,struct_time,exec_time.elapsed().as_micros(), &q7.len()), 1, debug_flag);
    }
    if query_list.contains(&8) {

        //Q8  MATCH (x: Person { test = 'pos '}) - / PREV* /-() -[: visits ]->(z:Class )
        let exec_time = Instant::now();
        let x = nodes.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let z = edges.iter().filter(|e| matches!(e.label,Label::visits)).map(|e| (e.src, (e.dst, e.time.clone()))).collect_vec();
        let q8 = hash_join(&z, &x);
        let struct_time = exec_time.elapsed().as_micros();
        let q8 = q8.into_iter()
            .filter(|((z, zt), x, xt)| zt.isPrev(&xt))
            .map(|((z, zt), x, xt)| (xt.toPoints().into_iter().cartesian_product(zt.toPoints()).filter(|(xt, zt)| *zt  <= *xt).map(move |(xt, zt)| ((x, xt), (z, zt)))))
            .flat_map(|x| x)
            .collect_vec();
        log(format!("q8[(x,t),(z,t)]{:?}", &q8), 5, debug_flag);
        log(format!("{:?},q8 ,{:?},{:?}, {:?}", data_name,struct_time,exec_time.elapsed().as_micros(), &q8.len()), 1, debug_flag);

    }


    if query_list.contains(&9) {


        // // MATCH (x: Person { risk = 'high '}) - / FWD /: meets / FWD / NEXT */ -({ test = 'pos '})
        //
        let exec_time = Instant::now();
        let x = nodes.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let y = nodes.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let mut zp = edges.iter().filter(|e| matches!(e.label,Label::meets)).map(|e| (e.src, e.clone())).collect_vec();

        let zpp = hash_join(&zp, &x).into_iter()
            .filter_map(|(e, _, n)| match e.time.overlap(&n) {
                true => Some((e.dst, (e.src, e.time.intersect(&n)))),
                false => None
            })
            .collect_vec();
        let zppp = hash_join(&zpp, &y).into_iter().map(|((x, xt), _, yt)| ((x, xt), (x, yt))).collect_vec();
        log(format!("zppp[(x,t)]{:?}", &zppp), 5, debug_flag);

        let struct_time = exec_time.elapsed().as_micros();
        let q9 = zppp.into_iter()
            .filter(|((x, xt), (_, yt))| yt.isNext(&xt))
            .map(|((x, xt), (_, yt))| (xt.toPoints().into_iter().cartesian_product(yt.toPoints()).filter(|(xt, yt)| *yt - *xt >= 0)).map(move |(xt, yt)| ((x, xt))))
            .flat_map(|x| x)
            .collect_vec();
        log(format!("q9[(x,t)]{:?}", &q9), 5, debug_flag);
        //log(format!("st, t, #q9:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_micros(), &q9.len()), 1, debug_flag);
        log(format!("{:?},q9 ,{:?},{:?}, {:?}", data_name,struct_time,exec_time.elapsed().as_micros(), &q9.len()), 1, debug_flag);

    }
    if query_list.contains(&10) {
        //     //Q10 MATCH (x:Person {risk = 'high'})- /FWD/:meets/FWD/PREV[0,12]-({test = 'pos'}) ON contact_tracing
        //
        let exec_time = Instant::now();
        let x = nodes.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.time.clone())).collect_vec();
        let y = nodes.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect_vec();
        let p = nodes.iter().filter(|p| matches!(p.label,Label::person)).map(|p| (p.nid, p.time.clone())).collect_vec();
        let y_pos = hash_join(&y, &p).into_iter()
            .filter_map(|(ypt, y, yt)| match yt.isBefore(&ypt) {
                true => Some((y, (yt,ypt))),
                false => None
            }).collect_vec(); // every person tested positive and their interval

        let mut zp = edges.iter()
            .filter(|e|  matches!(e.label,Label::meets) )
            .map(|e| (e.dst, e.clone())).collect_vec(); //all edges

        // all high risk and their edges
        let zpp = hash_join(&zp, &y_pos).into_iter().filter_map(|(e, _, (yt,ytp))|
            match ytp.isNext(&e.time)   {
                true => Some((e.src,(e.time,  ytp))),
                false => None
            }).collect_vec();


        let meets = hash_join(&zpp, &x).into_iter()
            .map(|((etime,ytp),x,xt)|(x,etime,ytp))
            .collect_vec();
        //log(format!("zppp[(x,t)]{:?}", &zppp), 5, debug_flag);
        let mut zppp = meets;

        let struct_time = exec_time.elapsed().as_micros();
        let q10 = zppp.into_iter()
            .map(|((x, xt, yt))| (xt.toPoints().into_iter().cartesian_product(yt.toPoints()).filter(|(xt, yt)| *yt - *xt <= qt.parse().unwrap())).map(move |(xt, yt)| (x, xt)))
            .flat_map(|x| x)
            .collect_vec();
        log(format!("10[(x,t)]{:?}", &q10), 5, debug_flag);
        //log(format!("st, t, #q10:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_micros(), &q10.len()), 1, debug_flag);
        log(format!("{:?},q10 ,{:?},{:?}, {:?}", data_name,struct_time,exec_time.elapsed().as_micros(), &q10.len()), 1, debug_flag);

    }

    if query_list.contains(&11) {
        //Q11 MATCH (x:Person {risk = 'high'})-// /FWD/:visits/FWD/:Room/BWD/:visits/// BWD/NEXT[0,12]/-({test = 'pos'})
        let exec_time = Instant::now();
        let x = nodes.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.time.clone())).collect_vec();
        let y = nodes.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect_vec();
        let p = nodes.iter().filter(|p| matches!(p.label,Label::person)).map(|p| (p.nid, p.time.clone())).collect_vec();
        let y_pos = hash_join(&y, &p).into_iter()
            .filter_map(|(ypt, y, yt)| match yt.isBefore(&ypt) {
                true => Some((y, (yt,ypt))),
                false => None
            }).collect_vec(); // every person tested positive and their interval


        let mut visit_edges = edges.iter().filter(|e|  matches!(e.label,Label::visits) ).map(|e| (e.src, e.clone())).collect_vec(); //all high risk who ride a bud
        let yppp = hash_join(&visit_edges, &y_pos).into_iter()
            .filter_map(|(e, _, (n,np))|  match e.time.isPrev(&np) {
                true => Some((e.dst, (e.src, e.time,np))),
                false => None
            }).collect_vec();

        let xpp = hash_join(&visit_edges, &x).into_iter()
            .filter_map(|(e, _, n)|  match e.time.overlap(&n) {
                true => Some((e.dst, (e.src, e.time))),
                false => None
            })

            .collect_vec();

        let mut room = hash_join(&xpp, &yppp).into_iter().filter_map(|((x,xt), _, (y,yt,ytp))| match xt.overlap(&yt) {
            true => Some(((x,xt.intersect(&yt),ytp))),
            false => None
        })
            .collect_vec();

        let zppp= room;
        //zppp.dedup();

        let struct_time = exec_time.elapsed().as_micros();
        let q11 = zppp.into_iter()
            .map(|((x, xt, yt))| (xt.toPoints().into_iter().cartesian_product(yt.toPoints()).filter(|(xt, yt)| *yt - *xt <= qt.parse().unwrap())).map(move |(xt, yt)| (x, xt)))
            .flat_map(|x| x)
            .collect_vec();
        log(format!("q11[(x,t)]{:?}", &q11), 5, debug_flag);
        //log(format!("st, t, #q11:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_micros(), &q11.len()), 1, debug_flag);
        log(format!("{:?},q11 ,{:?},{:?}, {:?}", data_name,struct_time,exec_time.elapsed().as_micros(), &q11.len()), 1, debug_flag);

    }


    if query_list.contains(&12) {

        //MATCH (x:Person {risk = 'high'})- /(FWD/:meets/FWD/NEXT[0,12]) + (FWD/:visits/FWD/:Room/BWD/:visits/ BWD/NEXT[0,12])/-({test = 'pos'})

        let exec_time = Instant::now();
        let x = nodes.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.time.clone())).collect_vec();
        let y = nodes.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect_vec();
        let p = nodes.iter().filter(|p| matches!(p.label,Label::person)).map(|p| (p.nid, p.time.clone())).collect_vec();
        let y_pos = hash_join(&y, &p).into_iter()
            .filter_map(|(ypt, y, yt)| match yt.isBefore(&ypt) {
                true => Some((y, (yt,ypt))),
                false => None
            }).collect_vec(); // every person tested positive and their interval

        let mut zp = edges.iter()
            .filter(|e|  matches!(e.label,Label::meets) )
            .map(|e| (e.dst, e.clone())).collect_vec(); //all edges

        // all high risk and their edges
        let zpp = hash_join(&zp, &y_pos).into_iter().filter_map(|(e, _, (yt,ytp))|
            match ytp.isNext(&e.time)   {
                true => Some((e.src,(e.time,  ytp))),
                false => None
            }).collect_vec();


        let meets = hash_join(&zpp, &x).into_iter()
            .map(|((etime,ytp),x,xt)|(x,etime,ytp))
            .collect_vec();
        //log(format!("zppp[(x,t)]{:?}", &zppp), 5, debug_flag);



        let mut visits_edges = edges.iter().filter(|e|  matches!(e.label,Label::visits) ).map(|e| (e.src, e.clone())).collect_vec(); //all high risk who ride a bud
        let yppp = hash_join(&visits_edges, &y_pos).into_iter()
            .filter_map(|(e, _, (n,np))|  match e.time.isPrev(&np) {
                true => Some((e.dst, (e.src, e.time,np))),
                false => None
            }).collect_vec();

        let xpp = hash_join(&visits_edges, &x).into_iter()
            .filter_map(|(e, _, n)|  match e.time.overlap(&n) {
                true => Some((e.dst, (e.src, e.time))),
                false => None
            })

            .collect_vec();

        let mut room = hash_join(&xpp,&yppp).into_iter().filter_map(|((x,xt), _, (y,yt,ytp))| match xt.overlap(&yt) {
            true => Some(((x,xt.intersect(&yt),ytp))),
            false => None
        })
            .collect_vec();

        let mut zppp = meets;
        zppp.append(&mut room);
        //zppp.dedup();

        let struct_time = exec_time.elapsed().as_micros();
        let q12 = zppp.into_iter()
            .map(|((x, xt, yt))| (xt.toPoints().into_iter().cartesian_product(yt.toPoints()).filter(|(xt, yt)| *yt - *xt <= qt.parse().unwrap())).map(move |(xt, yt)| (x, xt)))
            .flat_map(|x| x)
            .collect_vec();
        log(format!("q12[(x,t)]{:?}", &q12), 5, debug_flag);
        //log(format!("st, t, #q11:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_micros(), &q11.len()), 1, debug_flag);
        log(format!("{:?},q12 ,{:?},{:?}, {:?}", data_name,struct_time,exec_time.elapsed().as_micros(), &q12.len()), 1, debug_flag);

    }



}
