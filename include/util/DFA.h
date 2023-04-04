/*
**
* BEGIN_COPYRIGHT
*
* Copyright (C) 2016-2019 SciDB, Inc.
* All Rights Reserved.
*
* SciDB is free software: you can redistribute it and/or modify
* it under the terms of the AFFERO GNU General Public License as published by
* the Free Software Foundation.
*
* SciDB is distributed "AS-IS" AND WITHOUT ANY WARRANTY OF ANY KIND,
* INCLUDING ANY IMPLIED WARRANTY OF MERCHANTABILITY,
* NON-INFRINGEMENT, OR FITNESS FOR A PARTICULAR PURPOSE. See
* the AFFERO GNU General Public License for the complete license terms.
*
* You should have received a copy of the AFFERO GNU General Public License
* along with SciDB.  If not, see <http://www.gnu.org/licenses/agpl-3.0.html>
*
* END_COPYRIGHT
*/

/**
 * @file util/DFA.h
 * @author Mike Leibensperger <mjl@paradigm4.com>
 *
 * @brief Generic deterministic finite-state automaton (DFA) code
 *
 * @details This file implements several template classes that
 * cooperate to recognize regular expressions on an input language
 * whose symbols are objects of type T.
 *
 * - @c RE<T> is a regular expression node, either a Symbol<T> or a
 *   regular expression operator such as *, +, or ?.
 *
 * - @c Symbol<T> is the input language of T objects plus a few
 *   additional symbols used internally.
 *
 * - @c NFA<T> represents a non-deterministic finite-state automaton
 *   (NFA) over the Symbol<T> language using epsilon transitions.  (An
 *   epsilon transition is a state transition made "automatically"
 *   without consuming an input symbol.)
 *
 * - @c DFA<T> builds a deterministic finite-state automaton from the
 *   non-deterministic NFA<T>.  DFAs have no epsilon transitions and
 *   states have no more than one outbound transition per input
 *   symbol.
 *
 * Once a DFA<T> is built, it can recognize a sequence of objects of
 * type T, where T should:
 * - be a value type,
 * - be copy-constructible and default-constructible,
 * - have a 'std::ostream& operator<<(std::ostream&, T const&)' function,
 * - have operator<(T const&, T const&) and operator==(T const&, T const&).
 *
 * One good candidate for type T is OperatorParamPlaceholder (the
 * original intended use for this code).  Another possibility is
 * std::string, useful for unit testing this code.
 *
 * @see parser::Translator::matchParamsByRegex()
 * @see OperatorParamPlaceholder
 * @see query/DFA.h
 *
 * For general information about the algorithms here:
 * @see https://en.wikipedia.org/wiki/Thompson%27s_construction_algorithm
 * @see https://tajseer.files.wordpress.com/2014/06/re-nfa-dfa.pdf
 * @see http://www.cs.nuim.ie/~jpower/Courses/Previous/parsing/new-main.html
 * @see http://www.cs.nuim.ie/~jpower/Courses/Previous/parsing/node9.html
 */

#ifndef DETERMINISTIC_FINITE_STATE_AUTOMATON_H
#define DETERMINISTIC_FINITE_STATE_AUTOMATON_H

#include <deque>
#ifndef HAVE_DFA_HELPER_MACROS
#  include <iostream>
#endif
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#ifndef Assert
#  ifdef NDEBUG
//   Avoid "unused variable" warning.  Does what SCIDB_ASSERT() does,
//   without the #include dependencies.
#    define Assert(_cond)       do { (void)sizeof(_cond); } while (0)
#  else
#    include <cassert>
#    define Assert(_cond)       assert(_cond);
#  endif
#endif

namespace scidb { namespace dfa {

#ifndef HAVE_DFA_HELPER_MACROS

// Default helper macros for standalone use outside SciDB.

extern bool debug_flag;

#define DFA_DBG(_shifty)                                                \
do {                                                                    \
    if (dfa::debug_flag) {                                              \
        std::cout << "% " << __FUNCTION__ << ':' << __LINE__ << ": "    \
                  << _shifty << std::endl;                              \
    }                                                                   \
} while (0)

#define DFA_ERR(_shifty)                                                \
do {                                                                    \
    std::cerr << "ERR: " << __FUNCTION__ << ':' << __LINE__ << ": "     \
              << _shifty << std::endl;                                  \
} while (0)

#define DFA_FAIL(_shifty)                                       \
do {                                                            \
    std::string error;                                          \
    std::stringstream ss;                                       \
    ss << __FUNCTION__ << ':' << __LINE__ << ": " << _shifty;   \
    error = ss.str();                                           \
    Assert(false);                                              \
    std::cerr << error << std::endl;                            \
    ::exit(1);                                                  \
} while (0)

#endif /* ! HAVE_DFA_HELPER_MACROS */

// ----------------------------------------------------------------------
// Symbols and regular expressions on them
// ----------------------------------------------------------------------

/**
 * Augment the symbols of the input language with a few "impure" ones
 * of our own.
 *
 * SymType::PURE means this T value is a symbol of the input language.
 * Other SymType values are additional special symbols used by the
 * machinery below.
 */
template <typename T>
class Symbol
{
public:
    enum SymType { UNDEF, PURE, EPSILON, PUSH, POP };

    Symbol() : _stype(UNDEF), _symbol(T()) {}
    explicit Symbol(SymType t) : _stype(t), _symbol(T()) { Assert(t != UNDEF); }
    explicit Symbol(T const& t) : _stype(PURE), _symbol(t) {}

    SymType stype() const { return _stype; }
    T const& symbol() const { return _symbol; }

    bool isSpecial() const { return _stype != PURE; }
    bool isPure() const { return _stype == PURE; }

    static Symbol<T> makeEpsilon() { return Symbol<T>(EPSILON); }
    static Symbol<T> makePush() { return Symbol<T>(PUSH); }
    static Symbol<T> makePop() { return Symbol<T>(POP); }

private:
    SymType _stype { UNDEF };
    T _symbol;                  // T must have default ctor
};

template <typename T>
bool operator== (Symbol<T> const& left, Symbol<T> const& right)
{
    return left.stype() == right.stype()
        && left.symbol() == right.symbol();
}

template <typename T>
bool operator< (Symbol<T> const& left, Symbol<T> const& right)
{
    if (left.stype() < right.stype()) {
        return true;
    }
    if (left.stype() == right.stype()) {
        return left.symbol() < right.symbol();
    }
    return false;
}

template <typename T>
std::ostream& operator<< (std::ostream& os, Symbol<T> const& sym)
{
    switch (sym.stype()) {
    case Symbol<T>::UNDEF:
        os << "(undef)";
        break;
    case Symbol<T>::PURE:
        os << sym.symbol();
        break;
    case Symbol<T>::EPSILON:
        os << "(epsilon)";
        break;
    case Symbol<T>::PUSH:
        os << "(push)";
        break;
    case Symbol<T>::POP:
        os << "(pop)";
        break;
    }
    return os;
}

/**
 * RE<T> describes a regular expression on type T.
 *
 * The consistent() method assures that only correct regular
 * expressions can be built.
 */
template <typename T>
struct RE {
    using Vector = std::vector<RE>;

    enum Code {
        EMPTY = 0,      // empty symbol (useful as a child of OR)
        EPSILON = 0,    // Technical name for EMPTY
        LEAF,           // T (terminal, a real placeholder for type T)
        OR,             // T | U
        STAR,           // T*
        PLUS,           // T T*
        QMARK,          // T | epsilon
        LIST,           // T [ U [ V [ ... ]]]
        GROUP,          // '(' T ... ')'
    };
    static const char* strcode(Code c);

    // 'structors
    explicit RE(T const& t) : code(LEAF), sym(Symbol<T>(t)) {}
    explicit RE(Code c) : RE(c, {}) {}
    RE(Code c, Vector const& pv)
        : code(c), children(pv)
    {
        // Forbid inconsistent RE composition using induction.
        if (!consistent()) {
            DFA_FAIL("Bad RE node: " << *this);
        }
    }

    /**
     * Print RE as a proper regular expression, for help messages etc.
     *
     * @param sep  separator emitted between LIST or GROUP elements
     *
     * @note The use of 'sep' is kind of a hack to make generated
     *       help() text look better, but it isn't perfect.  For
     *       example, for a "xy?" regex it will produce "x, y?"
     *       instead of "x (, y)?".  Oh well.
     */
    std::string asRegex(std::string const& sep = std::string(" ")) const;

    /** Dump for debugging. */
    std::string asDump() const;

    /** Make sure nobody is abusing the Code values. */
    bool consistent() const;

    Code code { LEAF };
    Symbol<T> sym;
    Vector children;
};


template <typename T>
inline std::ostream& operator<< (std::ostream& os, RE<T> const& re)
{
    os << re.asRegex();
    return os;
}


template <typename T>
bool RE<T>::consistent() const
{
    // Get sym check out of the way.
    if (code == LEAF) {
        if (sym.isSpecial()) {
            DFA_ERR("Leaf cannot be special symbol");
            return false;
        }
        if (!children.empty()) {
            DFA_ERR("Leaf has children");
            return false;
        }
        return true;
    }

    if (sym.isPure()) {
        DFA_ERR("Non-leaf cannot store pure symbol");
        return false;
    }
    if (children.empty() && code != EPSILON) {
        DFA_ERR("Non-leaf has no children");
        return false;
    }

    switch (code) {
    case EPSILON:
    case LEAF:
        break;
    case OR:
        if (children.size() < 2) {
            DFA_ERR("OR has too few subexpressions");
            return false;
        }
        break;
    case STAR:
    case PLUS:
    case QMARK:
    case LIST:
        if (children.empty()) {
            DFA_ERR("STAR/PLUS/QMARK/LIST is childless");
            return false;
        }
        break;
    case GROUP:
        if (children.empty()) {
            DFA_ERR("GROUP cannot be empty");
            return false;
        }
        break;
    default:
        DFA_ERR("Code " << code << " has no consistency check");
        return false;
    }

    return true;
}

template <typename T>
const char* RE<T>::strcode(RE<T>::Code c)
{
    // Ugh.  Need .inc file?  Nah, too heavyweight.
    switch (c) {
    case EPSILON:   return "EPSILON";
    case LEAF:      return "LEAF";
    case OR:        return "OR";
    case STAR:      return "STAR";
    case PLUS:      return "PLUS";
    case QMARK:     return "QMARK";
    case LIST:      return "LIST";
    case GROUP:     return "GROUP";
    default:
        DFA_FAIL("You forgot to handle RE<T>::Code " << c);
        /*NOTREACHED*/
    };
}

template <typename T>
std::string RE<T>::asRegex(std::string const& sep_flavor) const
{
    Assert(consistent());
    std::stringstream result;

    auto stringifyChildren =
            [this, &sep_flavor](std::string const& sep, bool parens) {
        bool first = true;
        bool many = children.size() > 1 ||
                   (children.size() == 1 && children[0].code == GROUP);
        std::stringstream ss;
        if (many && parens) {
            ss << "( ";
        }
        for (auto const& child : children) {
            if (first) {
                first = false;
                ss << child.asRegex(sep_flavor);
            } else {
                ss << sep << child.asRegex(sep_flavor);
            }
        }
        if (many && parens) {
            ss << " )";
        }
        return ss.str();
    };

    switch (code) {

    case EPSILON:
        return "\"\"";

    case LEAF:
        result << sym;
        return result.str();

    case OR:
        // ( a | b | c )
        return stringifyChildren(" | ", true);

    case STAR:
        // ( ... )*
        // a*
        result << stringifyChildren(sep_flavor, true) << '*';
        return result.str();

    case PLUS:
        // ( ... )+
        // a+
        result << stringifyChildren(sep_flavor, true) << '+';
        return result.str();

    case QMARK:
        // ( ... )?
        // a?
        result << stringifyChildren(sep_flavor, true) << '?';
        return result.str();

    case LIST:
        // a b c
        return stringifyChildren(sep_flavor, false);

    case GROUP:
        // "(" a b c ")"       (double quotes so SciDB won\'t backquote \'em)
        result << "\"(\" " << stringifyChildren(sep_flavor, false) << " \")\"";
        return result.str();

    default:
        DFA_FAIL("You forgot to handle RE<T>::Code " << code);
        /*NOTREACHED*/
    }
}


template <typename T>
std::string RE<T>::asDump() const
{
    std::stringstream ss;

    if (code == RE<T>::LEAF) {
        ss << '"' << sym << '"';
        return ss.str();
    }
    ss << '{' << RE<T>::strcode(code);
    if (children.empty()) {
        ss << '}';
        return ss.str();
    }
    ss << ": ";
    bool first = true;
    for (auto const& c : children) {
        if (first) {
            first = false;
            ss << c;
        } else {
            ss << ", " << c;
        }
    }
    ss << '}';
    return ss.str();
}

template <typename T>
std::ostream& operator<< ( std::ostream& os, typename RE<T>::Vector const& v )
{
    os << '[';
    bool first = true;
    for (auto const& item : v) {
        if (first) {
            first = false;
            os << item;
        } else {
            os << ", " << item;
        }
    }
    os << ']';
    return os;
}


// ----------------------------------------------------------------------
// Regex-to-NFA
// ----------------------------------------------------------------------

// Set of NFA states, useful for naming a single DFA state among
// other uses.
using NfaStateSet = std::set<size_t>;

/**
 *  Non-deterministic Finite-state Automaton
 *
 *  This class builds a non-deterministic finite state automaton from
 *  a regular expression using Thompson's Construction Algorithm,
 *  which you can learn about on Wikipedia and in your favorite
 *  compiler textbook.
 */
template <typename T>
class NFA {

public:
    using Edge = std::pair<Symbol<T>,   // When you see this input symbol...
                           size_t>;     // ...go here (index into states[])

    struct State {
        bool final { false };
        std::vector<Edge> out;

        void addEdge(Symbol<T> const& sym, size_t dest)
        {
            out.push_back(std::make_pair(sym, dest));
        }
    };

    // Denotes a (sub)graph in the states[] table; item 0 is the start
    // state, items 1..N are the final state(s).
    using Graph = std::vector<size_t>;

    Graph graph;
    std::vector<State> states;
    std::set<Symbol<T>> terminals;  // Symbols seen so far, for DFA construction

private:
    // Recursive descent compilation of a placeholder regular expression.
    Graph doCompile(RE<T> const& regex);

    Graph onEpsilon();
    Graph onLeaf(RE<T> const& leaf);
    Graph onOr(std::vector<Graph> const& subgraphs);
    Graph onStar(Graph const& g);
    Graph onPlus(Graph const& g);
    Graph const& onQMark(Graph const& g);
    Graph onList(std::vector<Graph> const& subgraphs);
    Graph onGroup(std::vector<Graph> const& subgraphs);

    NfaStateSet vertices(Graph const& g) const;
    Graph duplicate(Graph const& g);

public:
    NFA() = default;
    ~NFA() = default;

    void reset()
    {
        graph.clear();
        states.clear();
        terminals.clear();
    }

    // Compile placeholder regular expression into this NFA.
    void compile(RE<T> const& regex)
    {
        reset();
        graph = doCompile(regex);
    }

    void dump(std::ostream& os) const;
};


template <typename T>
void NFA<T>::dump(std::ostream& os) const
{
    os << "-- Non-deterministic Finite-state Automaton at " << this << " --\n";

    for (size_t i = 0; i < states.size(); ++i) {
        os << "State[" << i << "]:";
        if (i == graph[0]) {
            os << " (START)\n";
        } else {
            bool found = false;
            for (size_t j = 1; j < graph.size(); ++j) {
                if (i == graph[j]) {
                    os << " (FINAL)\n";
                    found = true;
                    break;
                }
            }
            if (!found) {
                os << '\n';
            }
        }

        for (auto const& edge : states[i].out) {
            os << "    \"" << edge.first << "\" --> " << edge.second << '\n';
        }
    }

    os << "\nTerminals:\n";
    size_t const WRAP = 8;
    size_t i = 0;
    for (auto const& s : terminals) {
        os << '\t' << s;
        if (++i == WRAP) {
            i = 0;
            os << '\n';
        }
    }
    os << std::endl;
}


template <typename T>
typename NFA<T>::Graph
NFA<T>::doCompile(RE<T> const& regex)
{
    Assert(regex.consistent());
    std::vector<Graph> subgraphs;

    switch (regex.code) {

    case RE<T>::EPSILON:
        return onEpsilon();

    case RE<T>::LEAF:
        return onLeaf(regex);

    case RE<T>::OR:
        Assert(regex.children.size() > 1);
        for (RE<T> const& ph : regex.children) {
            subgraphs.push_back(doCompile(ph));
        }
        return onOr(subgraphs);

    case RE<T>::STAR:
        Assert(!regex.children.empty());
        if (regex.children.size() == 1) {
            return onStar(doCompile(regex.children[0]));
        } else {
            for (RE<T> const& ph : regex.children) {
                subgraphs.push_back(doCompile(ph));
            }
            return onStar(onList(subgraphs));
        }

    case RE<T>::PLUS:
        Assert(!regex.children.empty());
        if (regex.children.size() == 1) {
            return onPlus(doCompile(regex.children[0]));
        } else {
            for (RE<T> const& ph : regex.children) {
                subgraphs.push_back(doCompile(ph));
            }
            return onPlus(onList(subgraphs));
        }

    case RE<T>::QMARK:
        Assert(!regex.children.empty());
        if (regex.children.size() == 1) {
            return onQMark(doCompile(regex.children[0]));
        } else {
            for (RE<T> const& ph : regex.children) {
                subgraphs.push_back(doCompile(ph));
            }
            return onQMark(onList(subgraphs));
        }

    case RE<T>::LIST:
        if (regex.children.empty()) {
            return onEpsilon();
        }
        for (RE<T> const& c : regex.children) {
            subgraphs.push_back(doCompile(c));
        }
        return onList(subgraphs);

    case RE<T>::GROUP:
        Assert(regex.children.size() > 0);
        for (RE<T> const& c : regex.children) {
            subgraphs.push_back(doCompile(c));
        }
        return onGroup(subgraphs);

    default:
        DFA_FAIL("You forgot to handle RE<T>::Code " << regex.code);
        /*NOTREACHED*/
    }
}


template <typename T>
typename NFA<T>::Graph
NFA<T>::onEpsilon()
{
    Graph result;

    result.push_back(states.size()); // Initial state
    states.push_back(State());

    result.push_back(states.size()); // Final state
    states.push_back(State());
    states.back().final = true;

    // Epsilon (empty string) transition from start to final.
    states[result[0]].addEdge(Symbol<T>::makeEpsilon(), result[1]);

    return result;
}


template <typename T>
typename NFA<T>::Graph
NFA<T>::onLeaf(RE<T> const& leaf)
{
    Graph result;

    result.push_back(states.size()); // Initial state
    states.push_back(State());

    result.push_back(states.size()); // Final state
    states.push_back(State());
    states.back().final = true;

    // Symbol transition from start to final.
    states[result[0]].addEdge(leaf.sym, result[1]);

    // Remember the terminal symbols of the input language.
    terminals.insert(leaf.sym);

    return result;
}


template <typename T>
typename NFA<T>::Graph
NFA<T>::onOr(std::vector<Graph> const& subgraphs)
{
    Assert(subgraphs.size() > 1);
    Graph result;

    result.push_back(states.size()); // Initial state
    states.push_back(State());

    result.push_back(states.size()); // Final state
    states.push_back(State());
    states.back().final = true;

    for (auto const& g : subgraphs) {
        // Hook up subgraph start states with epsilon transitions.
        states[result[0]].addEdge(Symbol<T>::makeEpsilon(), g[0]);

        // For each subgraph final state, hook it up to new final
        // state and clear its 'final' flag.
        for (size_t i = 1; i < g.size(); ++i) {
            State& s = states[g[i]];
            Assert(s.final);
            s.final = false;
            s.addEdge(Symbol<T>::makeEpsilon(), result[1]);
        }
    }

    return result;
}


template <typename T>
typename NFA<T>::Graph
NFA<T>::onStar(Graph const& g)
{
    Graph result;

    result.push_back(states.size()); // Initial state
    states.push_back(State());

    result.push_back(states.size()); // Final state
    states.push_back(State());
    states.back().final = true;

    // Initial state epsilon-transitions to subgraph and to final
    // state.
    states[result[0]].addEdge(Symbol<T>::makeEpsilon(), result[1]);
    states[result[0]].addEdge(Symbol<T>::makeEpsilon(), g[0]);

    // Subgraph's final states epsilon-transition to new final state
    // and to their own start state.
    for (size_t i = 1; i < g.size(); ++i) {
        State& s = states[g[i]];
        Assert(s.final);
        s.final = false;
        s.addEdge(Symbol<T>::makeEpsilon(), result[1]);
        s.addEdge(Symbol<T>::makeEpsilon(), g[0]);
    }

    return result;
}


template <typename T>
typename NFA<T>::Graph
NFA<T>::onPlus(Graph const& g)
{
    std::vector<Graph> subgraphs;
    subgraphs.push_back(duplicate(g));
    subgraphs.push_back(onStar(g));
    return onList(subgraphs);
}


// Compute set of all veritices of graph descriptor 'g'.
template <typename T>
NfaStateSet NFA<T>::vertices(Graph const& g) const
{
    Assert(g.size() > 1);
    std::deque<size_t> todo;
    NfaStateSet result;

    // Induction step.
    todo.push_back(g[0]);
    result.insert(g[0]);

    while (!todo.empty()) {
        size_t v = todo.front();
        todo.pop_front();

        for (auto const& edge : states[v].out) {
            bool inserted = result.insert(edge.second).second;
            if (inserted) {
                todo.push_back(edge.second);
            }
        }
    }

    return result;
}


template <typename T>
typename NFA<T>::Graph
NFA<T>::duplicate(Graph const& g)
{
    Graph result(1);            // reserve first item for start
    std::map<size_t, size_t> clones; // original --> clone

    // Step 1: Clone all the states, without worrying about the edges.
    NfaStateSet verts = vertices(g);
    for (size_t v : verts) {
        size_t c = states.size();
        states.push_back(State());
        clones.insert(std::make_pair(v, c));
        states[c].final = states[v].final;
        if (states[c].final) {
            result.push_back(c); // record final states in graph descriptor
        }
    }
    result[0] = clones[g[0]];   // Now we know which clone is the start vertex.

    // Step 2: Clone all the edges.
    for (auto const& clone : clones) {
        for (auto const& edge : states[clone.first].out) {
            size_t target = clones[edge.second];
            states[clone.second].addEdge(edge.first, target);
        }
    }

    return result;
}


template <typename T>
typename NFA<T>::Graph const&
NFA<T>::onQMark(Graph const& g)
{
    // Note that the input 'g' in unchanged, but the graph it
    // describes *has* changed: it has new edges.  So although our
    // input and output are const, this method itself cannot be const.

    // Just add epsilon transition from start to all final states.
    for (size_t i = 1; i < g.size(); ++i) {
        states[g[0]].addEdge(Symbol<T>::makeEpsilon(), g[i]);
    }
    return g;
}


template <typename T>
typename NFA<T>::Graph
NFA<T>::onList(std::vector<Graph> const& sequence)
{
    // Easy cases first.
    if (sequence.empty()) {
        return onEpsilon();
    }
    if (sequence.size() == 1) {
        return sequence[0];
    }

    // Start state is first subgraph's start state.
    Graph result;
    result.push_back(sequence[0][0]);

    // Tie final state(s) of subgraph[i] to start state of
    // subgraph[i+1] with epsilon transitions.
    for (size_t i = 0; i < sequence.size() - 1; ++i) {
        Graph const& g = sequence[i];
        Graph const& h = sequence[i+1];

        for (size_t j = 1; j < g.size(); ++j) {
            State& s = states[g[j]];
            Assert(s.final);
            s.final = false;
            s.addEdge(Symbol<T>::makeEpsilon(), h[0]);
        }
    }

    // Resulting final state(s) are same as for last graph in the
    // sequence.
    for (size_t i = 1; i < sequence.back().size(); ++i) {
        result.push_back(sequence.back()[i]);
    }

    return result;
}


template <typename T>
typename NFA<T>::Graph
NFA<T>::onGroup(std::vector<Graph> const& group)
{
    Graph result;

    // Groups introduce these special symbols, needed for DFA
    // construction.
    terminals.insert(Symbol<T>::makePush());
    terminals.insert(Symbol<T>::makePop());

    result.push_back(states.size()); // Initial state
    states.push_back(State());

    result.push_back(states.size()); // Final state
    states.push_back(State());
    states.back().final = true;

    // Initial state PUSH-transitions to first subgraph's start state.
    states[result[0]].addEdge(Symbol<T>::makePush(), group[0][0]);

    // Tie final state(s) of subgraph[i] to start state of
    // subgraph[i+1] with epsilon transitions.
    for (size_t i = 0; i < group.size() - 1; ++i) {
        Graph const& g = group[i];
        Graph const& h = group[i+1];

        for (size_t j = 1; j < g.size(); ++j) {
            State& s = states[g[j]];
            Assert(s.final);
            s.final = false;
            s.addEdge(Symbol<T>::makeEpsilon(), h[0]);
        }
    }

    // Last subgraph's final states POP-transition to new final state.
    Graph const& last = group.back();
    for (size_t i = 1; i < last.size(); ++i) {
        State& s = states[last[i]];
        Assert(s.final);
        s.final = false;
        s.addEdge(Symbol<T>::makePop(), result[1]);
    }

    return result;
}


// ----------------------------------------------------------------------
// NFA-to-DFA
// ----------------------------------------------------------------------

/**
 *  Deterministic Finite-state Automaton
 *
 *  This class builds a deterministic finite-state automaton from a
 *  non-deterministic one using the "subset construction algorithm".
 *
 *  We don't perform "DFA minimisation" to eliminate vertices that are
 *  essentially duplicates, because we are lazy, pressed for time, and
 *  unconvinced that any reasonable LogicalOperator parameter spec
 *  would benefit from it.  But we could.  If we wanted to.
 *
 *  @see https://tajseer.files.wordpress.com/2014/06/re-nfa-dfa.pdf
 *  @see http://www.cs.nuim.ie/~jpower/Courses/Previous/parsing/node9.html
 *  @see http://www.cs.nuim.ie/~jpower/Courses/Previous/parsing/node10.html
 */
template <typename T>
class DFA
{
public:
    DFA() = default;
    explicit DFA(NFA<T> const& nfa) : _nfa(&nfa) { build(); }
    void build(NFA<T> const& nfa) { _nfa = &nfa; build(); }

    /// Dump as Graphviz/DOT, suitable for debugging or making pretty pictures.
    /// If state count exceeds the squeeze_threshold, tweak to fit better.
    /// @see http://www.graphviz.org/documentation/
    void asDot(std::ostream& os, size_t squeeze_threshold = 10) const;

    class Cursor {
        DFA const* _dfa { nullptr };
        size_t _state { 0 };
        bool _error { false };

    public:
        Cursor() = default;
        Cursor(Cursor const&) = default;
        Cursor& operator=(Cursor const&) = default;

        explicit Cursor(DFA const& dfa)
            : _dfa(&dfa)
        {
            // DFA must be built, not just default-constructed.
            Assert(!_dfa->dstates.empty());
        }

        bool move(Symbol<T> const& sym);
        void reset() { _state = 0; _error = false; }
        bool isAccepting() const { return _dfa && _dfa->dstates[_state].final; }
        bool isError() const { return _error; }
        bool isNull() const { return _dfa == nullptr; }
        std::vector<Symbol<T> > getExpected() const;
    };

    Cursor getCursor() const { return Cursor(*this); }

private:
    // Having computed the NfaStateSet name of a DFA state, does it
    // already exist in the dstates vector?
    using StateMap = typename std::map<NfaStateSet, size_t>;

    using Edge = typename NFA<T>::Edge;     // same same
    using EdgeMap = typename std::map<Symbol<T>, size_t>;

    struct DState {
        NfaStateSet name;
        EdgeMap out;
        bool final { false };

        DState() = delete;

        explicit DState(NFA<T> const& nfa, NfaStateSet const& ss)
            : name(ss)
        {
            // If any state in the NFA is final, this DFA state is
            // final too.
            for (auto const& s : ss) {
                if (nfa.states[s].final) {
                    final = true;
                    break;
                }
            }
        }
    };

    NFA<T> const* _nfa;         // Only valid during build()!
    StateMap stateMap;
    std::vector<DState> dstates;

    // Types, methods for "subset construction" of DFA from NFA.
    NfaStateSet moves(NfaStateSet const& states, Symbol<T> const& sym);
    NfaStateSet epsilonClosure(NfaStateSet const& states);
    NfaStateSet epsilonClosure(size_t stateIndex)
    {
        NfaStateSet oneState({stateIndex});
        return epsilonClosure(oneState);
    }
    void build();
};


inline std::ostream& operator<< (std::ostream& os, NfaStateSet const& nss)
{
    bool first = true;
    os << '{';
    for (auto const& s : nss) {
        if (first) {
            os << s;
            first = false;
        } else {
            os << ',' << s;
        }
    }
    os << '}';
    return os;
}


template <typename T>
void DFA<T>::build()
{
    dstates.clear();
    stateMap.clear();
    Assert(_nfa);

    // The initial dstate is the epsilon-closure of the NFA start state.
    NfaStateSet start = epsilonClosure(_nfa->graph[0]);
    dstates.push_back(DState(*_nfa, start));
    stateMap.insert(std::make_pair(start, 0));
    DFA_DBG("Starting DState is " << dstates[0].name);

    // Queue of DFA DStates to be analyzed.
    std::deque<size_t> todo;
    todo.push_back(0);

    while (!todo.empty()) {
        size_t const s = todo.front();
        Assert(s < dstates.size());
        todo.pop_front();
        DState* subject = &dstates[s];

        // For each language symbol, what's the next set of possible
        // NFA states?
        for (Symbol<T> const& sym : _nfa->terminals) {
            NfaStateSet next = epsilonClosure(moves(subject->name, sym));
            if (next.empty()) {
                // No outbound moves for sym, on to next terminal symbol.
                continue;
            }

            size_t dest;
            auto pos = stateMap.find(next);
            if (pos == stateMap.end()) {
                // A new DFA state is needed, create it and schedule
                // it for analysis.  (The DState ctor manages the
                // 'final' flag.)
                dest = dstates.size();
                dstates.push_back(DState(*_nfa, next));
                stateMap.insert(std::make_pair(next, dest));
                todo.push_back(dest);

                // The push_back may have reallocated, moving the subject.
                subject = &dstates[s];
            } else {
                dest = pos->second;
            }

            // Subject has out-edge to DState 'dest' via symbol 'sym'.
            bool inserted =
                subject->out.insert(std::make_pair(sym, dest)).second;
            Assert(inserted);
        }
    }

    // Done with NFA, don't leave dangling pointer.
    _nfa = nullptr;
}


// See http://www.graphviz.org/documentation/
template <typename T>
void DFA<T>::asDot(std::ostream& os, size_t squeeze_threshold) const
{
    os << "digraph dfa {\n"
       << "  node [shape=circle] ;\n" // default shape
       << "  rankdir = LR ;\n"
        ;

    // Hack, attempt to keep the graph on a single browser screen
    // without scrolling.
    if (dstates.size() > squeeze_threshold) {
        os << "  size = \"11,11\";\n"
           << "  ratio = 0.4 ;\n";
    }

    // Final states use doublecircle.
    for (size_t i = 0; i < dstates.size(); ++i) {
        if (dstates[i].final) {
            os << "  S" << i << " [shape=doublecircle];\n";
        }
    }

    // Start state S0 has charming shape!
    os << "  S0 [shape=oval] ;\n";

    // Edges!
    for (size_t i = 0; i < dstates.size(); ++i) {
        os << "\n  // --- S" << i;
        if (dstates[i].final) {
            os << " (FINAL) ---\n";
        } else {
            os << " ---\n";
        }
        os << "  // NFA states: " << dstates[i].name << '\n';
        for (auto const& edge : dstates[i].out) {
            os << "  S" << i << " -> S" << edge.second
               << " [label=\"" << edge.first << "\"] ;\n";
        }
    }

    os << "}\n";
}


template <typename T>
NfaStateSet DFA<T>::epsilonClosure(NfaStateSet const& states)
{
    Assert(_nfa);

    // Given a set of NFA states, compute the set of NFA states
    // reachable via epsilon transitions.

    NfaStateSet result(states);
    NfaStateSet todo(states);

    while (!todo.empty()) {
        NfaStateSet batch;
        batch.swap(todo);
        for (size_t s : batch) {
            typename NFA<T>::State const& state = _nfa->states[s];
            for (auto const& edge : state.out) {
                if (edge.first == Symbol<T>::makeEpsilon()) {
                    bool inserted = result.insert(edge.second).second;
                    if (inserted) {
                        todo.insert(edge.second);
                    }
                }
            }
        }
    }

    DFA_DBG(states << " --> " << result);

    return result;
}


template <typename T>
NfaStateSet DFA<T>::moves(NfaStateSet const& states, Symbol<T> const& sym)
{
    // Compute set of NFA states reachable from 'states' via symbol 'sym'.

    Assert(_nfa);
    NfaStateSet result;
    for (size_t s : states) {
        typename NFA<T>::State const& state = _nfa->states[s];
        for (auto const& edge : state.out) {
            if (edge.first == sym) {
                result.insert(edge.second);
            }
        }
    }

    DFA_DBG(states << " + \"" << sym << "\" --> " << result);

    return result;
}


template <typename T>
bool DFA<T>::Cursor::move(Symbol<T> const& sym)
{
    if (_error || !_dfa) {
        return false;
    }
    auto const& edge = _dfa->dstates[_state].out.find(sym);
    if (edge == _dfa->dstates[_state].out.end()) {
        _error = true;
        return false;
    }
    _state = edge->second;
    return true;
}


template <typename T>
std::vector<Symbol<T> > DFA<T>::Cursor::getExpected() const
{
    std::vector<Symbol<T> > result;
    if (!_error && _dfa) {
        for (auto const& edge : _dfa->dstates[_state].out) {
            result.push_back(edge.first);
        }
    }
    return result;
}

} } // namespace scidb::dfa

#ifndef HAVE_DFA_HELPER_MACROS
#  undef DFA_DBG
#  undef DFA_ERR
#  undef DFA_FAIL
#endif

#endif /* ! DETERMINISTIC_FINITE_STATE_AUTOMATON_H */
