package study.querydsl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.QMemberDto;
import study.querydsl.dto.UserDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceUnit;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static study.querydsl.entity.QMember.*;
import static study.querydsl.entity.QTeam.team;

@Transactional
@SpringBootTest
public class QuerydslBasicTest {
    @PersistenceContext
    EntityManager em;

    @Autowired
    JPAQueryFactory queryFactory;

    @BeforeEach
    void before() {
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamA);
        em.persist(teamB);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    void startJPQL() {
        // member1을 찾아라
        Member findMember = em.createQuery("select m from Member m where m.username = :username", Member.class)
                .setParameter("username", "member1")
                .getSingleResult();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    void startQuerydsl() {
        // member1을 찾아라
//        JPAQueryFactory queryFactory = new JPAQueryFactory(em); // querydsl을 쓸려면 해당 객체를 생성해야 함. -> entityManager로 queryFactory 생성
        // => JPAQueryFactory는 필드 레벨로 사용해도 된다.(권장) -> 여러 쓰레드에서 동시에 EM에 접근하여도, 트랜잭션마다 별도의 영속성 컨텍스트를 제공하기 때문에 동시성 문제는 걱정하지 않아도 된다.

//        QMember m = new QMember("m"); // compileQuerydsl을 해줘야 QEntity가 생김
        // => static import로 사용할 수 있다.
        // 같은 테이블을 조인하는 경우에만 직접 선언을 하여 alias를 설정("m")해서 사용하고, 보통은 static import로 사용한다.

        Member findMember = queryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1")) // 파라미터 바인딩
                .fetchOne(); // fetchOne(): 단건 조회

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    void search() {
        Member findMember = queryFactory
                .selectFrom(member) // .select().from()을 .selectFrom()으로 합칠 수 있다.
                .where(member.username.eq("member1").and(member.age.eq(10)))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    void searchAndParam() {
        Member findMember = queryFactory
                .selectFrom(member)
                .where(
                        member.username.eq("member1"), // .and()로 연결하는 것과 같음. 여러개가 들어 갈 수 있고 모두 and로 연결된다.
                        member.age.eq(10))
                .fetchOne();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    void resultFetch() {
        // list로 조회
        List<Member> fetch = queryFactory
                .selectFrom(member)
                .fetch();

        // 단건 조회
        Member fetchOne = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        // 처음 한 건 조회
        Member fetchFirst = queryFactory
                .selectFrom(member)
                .fetchFirst(); // == .limit(1).fetchOne();과 같다.

        // 페이징에서 사용
        QueryResults<Member> results = queryFactory
                .selectFrom(member)
                .fetchResults(); // count query, 전체 member 조회(contents용) 쿼리, 총 두번 발생
        results.getTotal();
        List<Member> content = results.getResults();

        // count 쿼리로 변경
        long total = queryFactory
                .selectFrom(member)
                .fetchCount();
    }

    /**
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순(desc)
     * 2. 회원 이름 오름차순(asc)
     * 단 2에서 회원 이름이 없으면 마지막에 출력 (nulls last)
     */
    @Test
    void sort() {
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);
        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();
    }

    @Test
    void paging1() {
        List<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1) // 0부터 시작
                .limit(2)
                .fetch(); // 쿼리 한번

        assertThat(result.size()).isEqualTo(2);
    }

    @Test
    void paging2() {
        QueryResults<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1) // 0부터 시작
                .limit(2)
                .fetchResults(); // 쿼리 두번

        assertThat(result.getTotal()).isEqualTo(4);
        assertThat(result.getLimit()).isEqualTo(2);
        assertThat(result.getOffset()).isEqualTo(1);
        assertThat(result.getResults().size()).isEqualTo(2);
    }

    @Test
    void aggregation() {
        // data type이 여러개 일때 튜플을 쓰는데, 실무에서는 보통 dto를 많이 사용한다.
        List<Tuple> result = queryFactory
                .select(
                        member.count(),
                        member.age.sum(),
                        member.age.avg(),
                        member.age.max(),
                        member.age.min()
                )
                .from(member)
                .fetch(); // QueryDsl의 Tuple 형식으로 반환

        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
        assertThat(tuple.get(member.age.max())).isEqualTo(40);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);

        for (Tuple tuple1 : result) {
            System.out.println("tuple = " + tuple1.get(0, Integer.class));
        }
    }

    /**
     * 팀의 이름과 각 팀의 평균 연령을 구해라.
     */
    @Test
    void group() {
        List<Tuple> result = queryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15);

        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(35);
    }

    /**
     * 팀 A에 소속된 모든 회원
     * => join(조인 대상, 별칭으로 사용할 Q타입)
     */
    @Test
    void join() {
        List<Member> result = queryFactory
                .selectFrom(member)
                .join(member.team, team) // innerjoin, leftjoin 등도 가능.
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("member1", "member2"); // containsExactly는 contains에서 순서, 중복, 종류의 일치 등이 추가 된다.
    }

    /**
     * 세타 조인: 연관관계가 없는 필드로 조인 -> 외부 조인(left join)이 안된다.
     * 회원의 이름이 팀 이름과 같은 회원 조회
     */
    @Test
    void theta_join() {
        em.persist(new Member("teamA")); // 회원 이름을 팀 이름으로 설정
        em.persist(new Member("teamB"));

        List<Member> result = queryFactory
                .select(member)
                .from(member, team) // theta join은 from 절에 조인 대상을 나열한다. -> 모든 회원과 모든 팀을 가지고 와서(DB가 성능 최적화를 할 것임)
                .where(member.username.eq(team.name)) // where절에서 필터링을 한다.
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("teamA", "teamB");
    }

    /**
     * 조인 -on절 -> 외부 조인 가능
     * ex) 회원과 팀을 조인하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
     * JPQL: select m, t from Member m left join m.team t on t.name = 'teamA';
     */
    @Test
    void join_on_filtering() {
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                .leftJoin(member.team, team).on(team.name.eq("teamA")) // member(left)를 기준으로 찾음. -> team이 null인 것도 조회된다.
                .fetch();

        // inner join에서는 on절을 쓰든 where절을 쓰든 결과가 똑같다. 익숙한 where절을 사용하는 걸 추천한다.
        // => leftjoin이 필요할 때 on절을 쓰자. (on절은 필터링하는 것. 조인하는 대상을 줄이는 것)
        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    /**
     * 연관관계가 없는 엔티티 외부 조인
     * 회원의 이름이 팀 이름과 같은 회원 조회
     */
    @Test
    void join_on_no_relation() {
        em.persist(new Member("teamA")); // 회원 이름을 팀 이름으로 설정
        em.persist(new Member("teamB"));
        em.persist(new Member("teamC"));

        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
                // 기본 leftJoin과는 문법이 다르다.
                .leftJoin(team).on(member.username.eq(team.name))
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @PersistenceUnit // entityManagerFactory 주입
    EntityManagerFactory emf;

    @Test
    void fetchJoinNo() {
        em.flush();
        em.clear(); // fetch join 테스트 시에는 영속성 컨텍스트의 데이터를 초기화하지 않으면 결과를 제대로 보기가 어렵다.

        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1"))
                .fetchOne(); // member만 조회됨.

        // member의 team이 이미 로딩된 엔티티 인지 아닌지(초기화가 안된 엔티티) 확인
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam()); // member의 team은 지연 로딩으로 설정되어 있음
        assertThat(loaded).as("페치 조인 미적용").isFalse();
    }

    @Test
    void fetchJoinUse() {
        em.flush();
        em.clear(); // fetch join 테스트 시에는 영속성 컨텍스트의 데이터를 초기화하지 않으면 결과를 제대로 보기가 어렵다.

        Member findMember = queryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin() // join 문법과 같은데, 뒤에 .fetchJoin()을 넣어주면 fetch join이 된다.
                .where(member.username.eq("member1"))
                .fetchOne(); // member만 조회됨.

        // member의 team이 이미 로딩된 엔티티 인지 아닌지(초기화가 안된 엔티티) 확인
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("페치 조인 적용").isTrue();
    }

    /**
     * 나이가 가장 많은 회원 조회
     */
    @Test
    void subQuery() {
        QMember memberSub = new QMember("memberSub"); // member의 alias를 다르게 설정하기 위해 QMember를 새로 생성

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        // sub query
                        JPAExpressions // static import 할 수 있음
                                .select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(40);
    }

    /**
     * 나이가 평균 이상인 회원
     */
    @Test
    void subQueryGoe() {
        QMember memberSub = new QMember("memberSub"); // member의 alias를 다르게 설정하기 위해 QMember를 새로 생성

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.goe( // 크거나 같은(Greater than or Equal to) >=
                        // sub query
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(30, 40);
    }

    /**
     * 서브 쿼리 여러건 처리, in 사용
     */
    @Test
    void subQueryIn() {
        QMember memberSub = new QMember("memberSub"); // member의 alias를 다르게 설정하기 위해 QMember를 새로 생성

        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.in(
                        // sub query
                        JPAExpressions
                                .select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10)) // >
                ))
                .fetch();

        assertThat(result).extracting("age")
                .containsExactly(20, 30, 40);
    }

    /**
     * select 절에 subquery 사용
     * JPA의 jpql 서브쿼리 한계점으로 from절의 서브쿼리(인라인 뷰)를 지원하지 않는다.
     * -> 당연히 querydsl도 지원하지 않음.
     */
    @Test
    void selectSubQuery() {
        QMember memberSub = new QMember("memberSub");

        List<Tuple> result = queryFactory
                .select(member.username,
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub)
                )
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    void basicCase() {
        List<String> result = queryFactory
                .select(member.age
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    void complexCase() {
        List<String> result = queryFactory
                .select(new CaseBuilder() // 복잡한 조건일 경우에 CaseBuilder 생성
                        .when(member.age.between(0, 20)).then("0~20살")
                        .when(member.age.between(21, 30)).then("21~30살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    void constant() {
        List<Tuple> result = queryFactory
                .select(member.username, Expressions.constant("A")) // username과 상수 값 "A"를 반환
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    void concat() {
        // 문자열 더하기 -> {username}_{age}
        List<String> result = queryFactory
                .select(member.username.concat("_").concat(member.age.stringValue())) // .stringValue(): 문자열로 타입 변환
                .from(member)
                .where(member.username.eq("member1"))
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    void simpleProjection() { // select절에 나열하는 것을 projection이라고 한다.
        List<String> result = queryFactory
                .select(member.username) // projection 대상이 하나
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    void tupleProjection() {
        List<Tuple> result = queryFactory
                .select(member.username, member.age) // projection 대상이 여러 개 일 때 튜플로 나옴
                .from(member)
                .fetch();

        for (Tuple tuple : result) {
            String username = tuple.get(member.username);
            Integer age = tuple.get(member.age);
            System.out.println("username = " + username);
            System.out.println("age = " + age);
        }
    }

    @Test
    void findDtoByJPQL() {
        List<MemberDto> result = em.createQuery("select new study.querydsl.dto.MemberDto(m.username, m.age)" +
                        " from Member m", MemberDto.class)
                .getResultList();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    void findDtoBySetter() { // property 접근 방법 (setter를 활용)
        List<MemberDto> result = queryFactory
                .select(Projections.bean(MemberDto.class, // dto에 기본 생성자를 만들어야 함
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    void findDtoByField() { // filed 접근 방법
        List<MemberDto> result = queryFactory
                .select(Projections.fields(MemberDto.class, // getter setter가 없어도 된다. 바로 값이 dto의 필드에 들어간다.
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    void findDtoByConstructor() { // 생성자 접근 방법
        List<MemberDto> result = queryFactory
                .select(Projections.constructor(MemberDto.class,
                        member.username,
                        member.age))
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    void findUserDtoByField() { // filed 접근 방법 - field명이 다를 경우 -> matching이 안되기 때문에 null로 나온다.
        QMember memberSub = new QMember("memberSub");

        List<UserDto> result = queryFactory
                .select(Projections.fields(UserDto.class,
                        member.username.as("name"), // 필드명이 맞아야 한다. -> as()로 맞춰줌
                        // subquery의 결과를 age에 매칭
                        ExpressionUtils.as(JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub), "age")))
                .from(member)
                .fetch();

        for (UserDto userDto : result) {
            System.out.println("memberDto = " + userDto);
        }
    }

    @Test
    void findUserDtoByConstructor() { // 생성자 접근 방법 - field명이 다를 경우 -> runtime 시점에 오류 발생
        List<UserDto> result = queryFactory
                .select(Projections.constructor(UserDto.class, // 기본 생성자도 있어야하고, 찾는 값을 매개로 가지고 있는 생성자도 있어야 함.
                        member.username, // 생성자는 타입으로 찾기 때문에 필드 이름 매칭은 상관 없음
                        member.age))
                .from(member)
                .fetch();

        for (UserDto userDto : result) {
            System.out.println("memberDto = " + userDto);
        }
    }

    @Test
    void findDtoByQueryProjection() { // @QueryProjection 방식
        // 장점: constructor와 비슷하지지만 compile 시점부터 오류를 찾을 수 있음. 가장 안전한 방식
        // 단점: Q타입을 생성해줘야 함. MemberDto자체가 qnerydsl에 대한 의존성을 가지게 됨.
        List<MemberDto> result = queryFactory
                .select(new QMemberDto(member.username, member.age)) // type이 맞지 않으면 compile 시점에 오류 발생
                .from(member)
                .fetch();

        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    @Test
    void dynamicQuery_BooleanBuilder() {
        String usernameParam = "member1";
        Integer ageParam = 10;

        List<Member> result = searchMember1(usernameParam, ageParam);
        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) { // cond: condition
        BooleanBuilder builder = new BooleanBuilder(); // 초기값을 넣어 줄 수도 있음.
        if (usernameCond != null) {
            builder.and(member.username.eq(usernameCond)); // and, or로 조립이 가능함
        }

        if (ageCond != null) {
            builder.and(member.age.eq(ageCond));
        }

        return queryFactory
                .selectFrom(member)
                .where(builder)
                .fetch();
    }


    @Test
    void dynamicQuery_WhereParam() {
        String usernameParam = "member1";
        Integer ageParam = 10;

        List<Member> result = searchMember2(usernameParam, ageParam);
        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember2(String usernameCond, Integer ageCond) {
        return queryFactory
                .selectFrom(member)
//                .where(usernameEq(usernameCond), ageEq(ageCond))
                .where(allEq(usernameCond, ageCond)) // 조립이 가능하다는 장점이 있고, 재사용이 가능하다.
                .fetch();
    }

    private BooleanExpression usernameEq(String usernameCond) { // 조립하려면 반환값이 Predicate가 아니라 BooleanExpression이어야 한다.
        // 응답값이 null이면 무시된다.
        return usernameCond != null ? member.username.eq(usernameCond) : null; // 긍정적인 걸 먼저 체크 하는게 낮다고 함
    }

    private BooleanExpression ageEq(Integer ageCond) {
        return ageCond != null ? member.age.eq(ageCond) : null;
    }

    // 활용 예시: 광고 상태 isValid, 날짜 In -> isServicable
    private BooleanExpression allEq(String usernameCond, Integer ageCond) {
        // 이 경우에는 null처리를 챙겨줘야 한다.
        return usernameEq(usernameCond).and(ageEq(ageCond));
    }

    @Test
    void bulkUpdate() {
        // member1 = 10 -> 비회원
        // member2 = 20 -> 비회원
        // member3 = 30 -> 유지
        // member4 = 40 -> 유지

        long count = queryFactory
                .update(member)
                .set(member.username, "비회원")
                .where(member.age.lt(28)) // < 28
                .execute();

        System.out.println("count = " + count);

        // bulk 연산 후 영속성 컨텍스트를 초기화 한다. (bulk연산 한 값이 영속성 컨텍스트에 적용되지 않기 때문)
        em.flush();
        em.clear();

        List<Member> result = queryFactory
                .selectFrom(member)
                .fetch();

        for (Member member1 : result) {
            System.out.println("member1 = " + member1);
        }
    }

    @Test
    void bulkAdd() {
        long count = queryFactory
                .update(member)
                .set(member.age, member.age.add(1)) // 곱하기는 multiply(2)
                .execute();
    }

    @Test
    void bulkDelete() {
        long count = queryFactory
                .delete(member)
                .where(member.age.gt(18))
                .execute();
    }

    @Test
    void sqlFunction() { // member -> M으로 변경하는 replace 함수 사용
        List<String> result = queryFactory
                .select(Expressions.stringTemplate(
                        "function('replace', {0}, {1}, {2})",
                        member.username, "member", "M"))
                .from(member)
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    void sqlFunction2() {
        List<String> result = queryFactory
                .select(member.username)
                .from(member)
//                .where(member.username.eq(
//                        Expressions.stringTemplate("function('lower', {0})", member.username)
//                ))
                .where(member.username.eq(member.username.lower())) // 모든 db에서 기본적으로 제공하는 ansi 표준 함수들은 querydsl에 내장되어 있다.
                .fetch();

        for (String s : result) {
            System.out.println("s = " + s);
        }
    }
}
