extern crate tpath;
use itertools::Itertools;

use tpath::interval;
use tpath::interval::Interval;
use tpath::node::Node;
use tpath::edge::{Edge};
use std::collections::HashMap;
use std::ops::Index;
use std::ptr::null;
use tpath::join::hash_join;
use tpath::label::Label;
use std::time::Instant;


fn main() {

    //let a = [1,2,3,4,6,7,8,9,10,11];
    let a = [11];
    fn log(input: String, level: usize, debug_flag: usize) {
        if debug_flag >= level {
            println!("{:?}", input);
        }
    }

    let debug_flag = 3;

    let n = Node::get_from_file("/Users/amir/Documents/Projects/rust/tpath/data/5min/node.csv");
    log(format!("Nodes{:?}", &n), 10, debug_flag);

    let e = Edge::get_from_file("/Users/amir/Documents/Projects/rust/tpath/data/5min/edge.csv");
    log(format!("Edges{:?}", &e), 10, debug_flag);


    if a.contains(&1) {
        //Q1 MATCH (x: Person) ON contact_tracing
        let exec_time = Instant::now();
        let q1 = n.iter().filter(|p| matches!(p.label,Label::person))
            .flat_map(|p| (p.time.zip(p.nid)))
            .collect_vec();

        log(format!("q1(x,t){:?}", &q1), 5, debug_flag);
        log(format!("t, #q1 {:?}, {:?}", exec_time.elapsed().as_millis(), &q1.len()), 1, debug_flag);
    }
    if a.contains(&2) {
        //Q2 MATCH (x: Person { risk = 'low '}) ON contact_tracing
        let exec_time = Instant::now();
        let q2 = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::low)).collect_vec();
        let struct_time = exec_time.elapsed().as_millis();
        let q2 = q2.iter()
            .flat_map(|p| (p.time.zip(p.nid))).collect_vec();

        log(format!("st, t, #q2:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q2.len()), 1, debug_flag);
        log(format!("t, #q2 {:?}, {:?}", exec_time.elapsed().as_millis(), &q2.len()), 1, debug_flag);
    }


    if a.contains(&3) {
        //Q3 MATCH (x: Person { risk = 'low ' AND time = '1'}) ON contact_tracing
        let exec_time = Instant::now();
        let q3 = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::low))
            .flat_map(|p| (p.time.zip(p.nid)))
            .filter(|(pid, time)| *time == 1)
            .collect_vec();
        log(format!("q3(x,t){:?}", &q3), 5, debug_flag);
        log(format!("t, #q3 {:?}, {:?}", exec_time.elapsed().as_millis(), &q3.len()), 1, debug_flag);
    }

    if a.contains(&4) {
        //Q4 MATCH (x: Person { risk = 'low ' AND time < '10'}) ON contact_tracing
        let exec_time = Instant::now();
        let q4 = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::low))
            .flat_map(|p| (p.time.zip(p.nid)))
            .filter(|(pid, time)| *time < 10)
            .collect_vec();
        log(format!("q4(x,t){:?}", &q4), 5, debug_flag);
        log(format!("t, #q4 {:?}, {:?}", exec_time.elapsed().as_millis(), &q4.len()), 1, debug_flag);
    }

    if a.contains(&5) {
        /* Q5 MATCH (x: Person { risk = 'low '}) -[z: meets ]->(y: Person { risk = 'high '}) */
        let exec_time = Instant::now();
        let x = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::low)).map(|p| (p.nid, p.clone())).collect_vec();
        let y = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.clone())).collect_vec();
        let mut zp = e.iter().filter(|e| matches!(e.label,Label::meets)).map(|e| (e.src, e.clone())).collect_vec();
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
        let struct_time = exec_time.elapsed().as_millis();

        let q5 = q5.iter()
            .flat_map(|e| (e.time.zip_vec(&vec![e.src, e.eid, e.dst])))
            //.map(|e|(e.time.zip(e.src),(e.time.zip(e.eid)),(e.time.zip(e.dst))))
            .collect_vec();
        log(format!("q5[(x,t),(z,t),(y,t)]{:?}", &q5), 5, debug_flag);
        log(format!("st, t, #q5:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q5.len()), 1, debug_flag);
    }


// Q6 MATCH (x: Person { test = 'pos '}) - / PREV /-(y: Person ) ON contact_tracing
    if a.contains(&6) {
        let exec_time = Instant::now();
        let x = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos))/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let q6 = hash_join(&n.iter().map(|p| (p.nid, p.time.clone())).collect_vec(), &x.map(|x| (x.nid, x.time.clone())).collect_vec());
        let struct_time = exec_time.elapsed().as_millis();
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
        log(format!("st, t, #q6:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q6.len()), 1, debug_flag);
    }


    if a.contains(&7) {

        // Q7 MATCH (x: Person { test = 'pos '}) -     / PREV /-() -[: rides ]->(z:Bus )
        let exec_time = Instant::now();
        let x = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let z = e.iter().filter(|e| matches!(e.label,Label::rides)).map(|e| (e.src, (e.dst, e.time.clone()))).collect_vec();
        let q7 = hash_join(&z, &x);
        let struct_time = exec_time.elapsed().as_millis();
        let q7 = q7.into_iter()
            .filter_map(|((z, zt), x, xt)|
                match zt.prev(&xt) {
                    None => None,
                    Some(zt) => Some((xt.toPoints().into_iter().cartesian_product(zt.toPoints()).filter(|(xt, zt)| *xt - 1 == *zt).map(move |(xt, zt)| ((x, xt), (z, zt)))))
                })
            .flat_map(|x| x)
            .collect_vec();
        log(format!("q7[(x,t),(z,t)]{:?}", &q7), 5, debug_flag);
        log(format!("st, t, #q7:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q7.len()), 1, debug_flag);


        // // Q7 MATCH (x: Person { test = 'pos '}) -     / PREV /-() -[: rides ]->(z:Bus )
        // let exec_time = Instant::now();
        // let x = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        // let z = e.iter().filter(|e| matches!(e.label,Label::rides)).map(|e| (e.src, (e.dst, e.time.clone()))).collect_vec();
        // let q7 = hash_join(&z, &x);
        // let struct_time = exec_time.elapsed().as_millis();
        // let q7 = q7.into_iter()
        //     .filter_map(|((z, zt), x, xt)|
        //         match zt.isPrev(&xt) {
        //             false => None,
        //             true => Some((xt.toPoints().into_iter().cartesian_product(zt.toPoints()).filter(|(xt, zt)| *xt - 1 == *zt).map(move |(xt, zt)| ((x, xt), (z, zt)))))
        //         })
        //     .flat_map(|x| x)
        //     .collect_vec();
        // log(format!("q7'[(x,t),(z,t)]{:?}", &q7), 5, debug_flag);
        // log(format!("st, t, #q7':{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q7.len()), 1, debug_flag);
    }
    if a.contains(&8) {

        //Q8  MATCH (x: Person { test = 'pos '}) - / PREV* /-() -[: rides ]->(z:Bus )
        let exec_time = Instant::now();
        let x = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let z = e.iter().filter(|e| matches!(e.label,Label::rides)).map(|e| (e.src, (e.dst, e.time.clone()))).collect_vec();
        let q8 = hash_join(&z, &x);
        let struct_time = exec_time.elapsed().as_millis();
        let q8 = q8.into_iter()
            .filter(|((z, zt), x, xt)| zt.isPrev(&xt))
            .map(|((z, zt), x, xt)| (xt.toPoints().into_iter().cartesian_product(zt.toPoints()).filter(|(zt, xt)| *zt - *xt >= 0).map(move |(xt, zt)| ((x, xt), (z, zt)))))
            .flat_map(|x| x)
            .collect_vec();
        log(format!("q8[(x,t),(z,t)]{:?}", &q8), 5, debug_flag);
        log(format!("st, t, #q8:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q8.len()), 1, debug_flag);
    }


    if a.contains(&9) {


        // // MATCH (x: Person { risk = 'high '}) - / FWD /: meets / FWD / NEXT */ -({ test = 'pos '})
        //
        let exec_time = Instant::now();
        let x = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let y = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let mut zp = e.iter().filter(|e| matches!(e.label,Label::meets)).map(|e| (e.src, e.clone())).collect_vec();

        let zpp = hash_join(&zp, &x).into_iter()
            .filter_map(|(e, _, n)| match e.time.overlap(&n) {
                true => Some((e.dst, (e.src, e.time.intersect(&n)))),
                false => None
            })
            .collect_vec();
        let zppp = hash_join(&zpp, &y).into_iter().map(|((x, xt), _, yt)| ((x, xt), (x, yt))).collect_vec();
        log(format!("zppp[(x,t)]{:?}", &zppp), 5, debug_flag);

        let struct_time = exec_time.elapsed().as_millis();
        let q9 = zppp.into_iter()
            .filter(|((x, xt), (_, yt))| yt.isNext(&xt))
            .map(|((x, xt), (_, yt))| (xt.toPoints().into_iter().cartesian_product(yt.toPoints()).filter(|(xt, yt)| *yt - *xt >= 0)).map(move |(xt, yt)| ((x, xt), (x, yt))))
            .flat_map(|x| x)
            .collect_vec();
        log(format!("q9[(x,t)]{:?}", &q9), 5, debug_flag);
        log(format!("st, t, #q9:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q9.len()), 1, debug_flag);
    }
    if a.contains(&10) {
        //     //Q10 MATCH (x:Person {risk = 'high'})- /FWD/:meets/FWD/PREV[0,24]-({test = 'pos'}) ON contact_tracing
        //
        let exec_time = Instant::now();
        let x = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let y = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let mut zp = e.iter().filter(|e| matches!(e.label,Label::meets)).map(|e| (e.src, e.clone())).collect_vec();

        let zpp = hash_join(&zp, &x).into_iter()
            .filter_map(|(e, _, n)| match e.time.overlap(&n) {
                true => Some((e.dst, (e.src, e.time.intersect(&n)))),
                false => None
            })
            .collect_vec();
        let zppp = hash_join(&zpp, &y).into_iter().map(|((x, xt), _, yt)| ((x, xt), (x, yt))).collect_vec();

        let struct_time = exec_time.elapsed().as_millis();
        let q10 = zppp.into_iter()
            .filter(|((x, xt), (_, yt))| yt.isPrev(&xt))
            .map(|((x, xt), (_, yt))| (xt.toPoints().into_iter().cartesian_product(yt.toPoints()).filter(|(xt, yt)| *xt - *yt >= 0 && *xt - *yt <= 24)).map(move |(xt, yt)| ((x, xt), (x, yt))))
            .flat_map(|x| x)
            .collect_vec();
        log(format!("10[(x,t)]{:?}", &q10), 5, debug_flag);
        log(format!("st, t, #q10:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q10.len()), 1, debug_flag);
    }

    if a.contains(&11) {


        // Q11 MATCH MATCH (x:Person {risk = 'high'})-/((FWD/:meets/FWD) + (FWD/:rides/FWD/:Bus/BWD/:rides/BWD))/NEXT[0,24]/-({test = 'pos'})
        //
        let exec_time = Instant::now();
        let x = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop2,Label::high)).map(|p| (p.nid, p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let y = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid, p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
        let mut zp = e.iter().filter(|e|  matches!(e.label,Label::meets) ).map(|e| (e.src, e.clone())).collect_vec();

        let zpp = hash_join(&zp, &x).into_iter()
            .filter_map(|(e, _, n)| match e.time.overlap(&n) {
                true => Some((e.dst, (e.src, e.time.intersect(&n)))),
                false => None
            })
            .collect_vec();
        let meets = hash_join(&zpp, &y).into_iter().map(|((x, xt), _, yt)| ((x, xt), (x, yt))).collect_vec();
        //log(format!("zppp[(x,t)]{:?}", &zppp), 5, debug_flag);

        let mut ep = e.iter().filter(|e|  matches!(e.label,Label::rides) ).map(|e| (e.src, e.clone())).collect_vec();
        let xpp = hash_join(&ep, &x).into_iter()
            .filter_map(|(e, _, n)| match e.time.overlap(&n) {
                true => Some((e.dst, (e.src, e.time.intersect(&n)))),
                false => None
            })
            .collect_vec();

        let ypp = hash_join(&ep, &y).into_iter()
            .filter_map(|(e, _, n)| match e.time.overlap(&n) {
                true => Some((e.dst, (e.src, e.time.intersect(&n)))),
                false => None
            })
            .collect_vec();

        let zp = hash_join(&xpp,&ypp).into_iter().filter_map(|((x,xt), _, (y,yt))| match xt.overlap(&yt) {
            true => Some((y, (x,xt,yt))),
            false => None
        })
            .collect_vec();

        let mut bus = hash_join(&y,&zp).into_iter()
            .map(|(yt,_,(x,xt,_))|((x,xt),(x,yt)))
            .collect_vec();
        let mut zppp = meets;
        zppp.append(&mut bus);

        let struct_time = exec_time.elapsed().as_millis();
        let q11 = zppp.into_iter()
            .filter(|((x, xt), (_, yt))| yt.isNext(&xt))
            .map(|((x, xt), (_, yt))| (xt.toPoints().into_iter().cartesian_product(yt.toPoints()).filter(|(xt, yt)| *yt - *xt >= 0)).map(move |(xt, yt)| ((x, xt), (x, yt))))
            .flat_map(|x| x)
            .collect_vec();
        log(format!("q11[(x,t)]{:?}", &q11), 5, debug_flag);
        log(format!("st, t, #q11:{:?}, {:?}, {:?}", struct_time, exec_time.elapsed().as_millis(), &q11.len()), 1, debug_flag);
    }

}



// // //Q8  MATCH (x: Person { test = 'pos '}) - / PREV* /-() -[: rides ]->(z:Bus )
// let exec_time =  Instant::now();
// let x = n.iter().filter(|p| matches!(p.label,Label::person) && matches!(p.prop3,Label::pos)).map(|p| (p.nid,p.time.clone())).collect_vec()/*.flat_map(|p| (p.time.zip(p.nid))).collect_vec()*/;
// let z = e.iter().filter(|e| matches!(e.label,Label::rides)).map(|e| (e.src, (e.dst, e.time.clone()))).collect_vec();
// let q8 = hash_join(&z,&x);
// let struct_time = exec_time.elapsed().as_millis();
//
// let mut flag = 1;
// let mut zp = q8;
//     let mut q8_o:Vec<((usize,usize),(usize,usize))>=vec![];
// while zp.len() > 0 {
//
//     log(format!("zp[(x,t),(z,t)]{:?}", &zp), 5,debug_flag);
//
//
//     let  dQ =     zp.clone().into_iter()
//         .filter_map(|((z, zt), x, xt)|
//             match zt.prev(&xt) {
//                 None => None,
//                 Some(zt) => Some((xt.toPoints().into_iter().cartesian_product(zt.toPoints()).filter(|(xt, zt)| *zt - *xt >= 0).map(move |(xt, zt)| ((x, xt), (z, zt)))))
//             })
//         .flat_map(|x| x)
//         .collect_vec();
//
//     zp = zp.into_iter()
//         .filter_map(|((z,zt),x,xt)|
//             match zt.prev(&zt) {
//                 None => None,
//                 Some(ztt) =>  Some(((z,ztt),x,xt))
//             })
//         //.flat_map(|x| x)
//         .collect_vec();
//     flag = flag + 1;
//     q8_o.append(&mut dQ.clone());
//
// }
//
// log(format!("q8[(x,t),(z,t)]{:?}", &q8_o), 5,debug_flag);
// log(format!("st, t, #q8:{:?}, {:?}, {:?}",struct_time,exec_time.elapsed().as_millis(), &q8_o.len()), 1,debug_flag);
