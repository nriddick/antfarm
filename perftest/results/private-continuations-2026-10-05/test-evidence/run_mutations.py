#!/usr/bin/env python3
"""Run serial E2-4 mutants from the canonical uncommitted workspace; always restore.
Use: python3 perftest/results/private-continuations-2026-10-05/test-evidence/run_mutations.py [C-M...]
No canonical binaries are rebuilt or used while a mutant is live.
"""
import difflib, hashlib, json, os, pathlib, subprocess, sys, time
root = pathlib.Path(__file__).resolve().parents[4]
os.chdir(root)
runtime = pathlib.Path('actors/private_actor.d')
tests = pathlib.Path('private_actor_tests/main.d')
evidence = pathlib.Path(__file__).resolve().parent / 'mutations'
evidence.mkdir(exist_ok=True)
canonical = runtime.read_text(); canonical_tests = tests.read_text()
env = os.environ.copy(); env.update(ANTFARM_HUGE_PAGES='0', TSAN_OPTIONS='history_size=7:halt_on_error=1')

def replace(s, a, b):
    assert s.count(a) == 1, (a[:100], s.count(a))
    return s.replace(a, b)

def mutate(name):
    s = canonical; t = canonical_tests
    if name == 'C-M1':
        s = replace(s, 'case ChildStep.waitChildren:\n            pushChildReturn(member);', 'case ChildStep.waitChildren:\n            auto lostWave = member.enclosing;\n            member.childFlags = 0;\n            releaseWave(lostWave);')
    elif name == 'C-M2':
        s = replace(s, '            ChildStep next;\n            if (disposition', '            if (disposition == PrivateChildDisposition.waitChildren\n                && atomicLoad!(MemoryOrder.acq)(entry.childPending) == 1)\n                return runChild!(operation, subset)(null, body, 0);\n            ChildStep next;\n            if (disposition')
    elif name == 'C-M3':
        s = replace(s, '            releaseMember(entry); // custody release: final Entry access unless owner', '            version (AntfarmActorTestHooks)\n                actorTestPoint(ActorTestPoint.privateChildCustodyBeforeRelease, null);\n            atomicFetchSub!(MemoryOrder.acq_rel)(entry.childPending, 1u); // no final-owner handling')
    elif name == 'C-M4':
        s = replace(s, '            releaseMember(parent); // final parent access; idle alone does not pin Entry', '            atomicFetchSub!(MemoryOrder.acq_rel)(parent.childPending, 1u); // no final-owner handling')
    elif name in ['C-M5a', 'C-M5b']:
        s = replace(s, '                atomicFetchAdd!(MemoryOrder.acq_rel)(wave.pending, 1UL);\n                entry.childFlags = childHolding;', '                entry.childFlags = childHolding;')
        if name == 'C-M5b':
            s = replace(s, '                actorTestPoint(ActorTestPoint.privateChildCustodyAfterRelease, null);\n        }\n        else\n        {\n            auto borrow', '                actorTestPoint(ActorTestPoint.privateChildCustodyAfterRelease, null);\n            if (!(flags & childHolding))\n                atomicFetchAdd!(MemoryOrder.acq_rel)(wave.pending, 1UL); // too late\n        }\n        else\n        {\n            auto borrow')
    elif name == 'C-M6':
        s = replace(s, '            releaseWave(wave); // drop the activation hold as the last owner access', '            // lost activation hold release')
    elif name == 'C-M7':
        s = replace(s, '    bool resumeScheduled;\n', '    bool resumeScheduled;\n    void function(Service*, Entry*) nothrow @nogc @system inlineRetire; // mutant only\n')
        s = replace(s, '        park.base_.dispose = &dispose;', '        park.base_.dispose = &dispose;\n        park.base_.inlineRetire = &retireMember!T;')
        s = replace(s, '            member.childFlags |= childCleanup;\n            pushChildReturn(member);', '            auto retiringBase = ChildBase.fromWave(member.enclosing);\n            auto retiringWave = member.enclosing;\n            retiringBase.inlineRetire(retiringBase.service, member);\n            member.childFlags = 0;\n            releaseWave(retiringWave);')
        t = replace(t, 'static assert(PVoidCohort.sizeof == 336);', 'static assert(PVoidCohort.sizeof == 344); // mutant-only erased callback')
    elif name == 'C-M7b':
        s = replace(s, '            if (flags & childCleanup)\n            {\n                retireMember!T', '            if (flags & childCleanup)\n            {\n                auto cleanupBorrow = PrivateBorrow!T(cast(T*) entry.state);\n                auto cleanupContext = PrivateContext(entry, wave, false, park.batchLimit_);\n                operation(cleanupBorrow, cleanupContext); // erroneous user activation\n                retireMember!T')
    elif name == 'C-M8':
        a = s.index('    bool prepareTableFrontN(size_t n)', s.index('private struct ResumeBodies'))
        b = s.index('\n}\n\n/// Opaque', a)
        s = s[:a] + '    bool prepareTableFrontN(size_t n) nothrow @nogc @system { return true; }\n' + s[b:]
        s = replace(s, '        if (written == 0) releaseWave(wave); // P still pins the park', '        if (written != 0)\n        {\n            auto next = bodies.cursor;\n            foreach (_; 0 .. written) next = next.next; // accepted Entries are already published\n            base.resumeHead = next;\n            base.resumeCount -= written;\n            if (base.resumeCount == 0) base.resumeTail = null;\n        }\n        if (written == 0) releaseWave(wave); // P still pins the park')
    elif name == 'C-M8b':
        s = replace(s, '        if (written == 0) releaseWave(wave); // P still pins the park', '        // leaked speculative table credit on zero admission')
    elif name == 'C-M9a':
        s = replace(s, '        if (base.resumeCount != 0) return false;', '        releaseWave(wave); // P released before FIFO check / unscheduling\n        if (base.resumeCount != 0) return false;')
        s = replace(s, '        releaseWave(wave); // drop P: final park access', '        // P was already dropped')
    elif name == 'C-M9b':
        s = replace(s, '        base.resumeScheduled = false;\n        releaseWave(wave); // drop P: final park access', '        releaseWave(wave); // P dropped before final metadata write')
        s = replace(s, '            actorTestPoint(ActorTestPoint.privateResumeAfterUnschedule, null);\n        return true;', '            actorTestPoint(ActorTestPoint.privateResumeAfterUnschedule, null);\n        base.resumeScheduled = false; // freed park under C9b schedule\n        return true;')
    elif name == 'C-M10':
        s = replace(s, '            ChildStep next;\n            if (disposition', '            static if (kind == OpKind.resultContinuation)\n                if (disposition == PrivateChildDisposition.waitChildren)\n                {\n                    static if (subset)\n                        park.writeResult(park.sel_[park.capacity_ + (entry - park.entries_)], step.value_);\n                    else park.writeResult(entry - park.entries_, step.value_);\n                }\n            ChildStep next;\n            if (disposition')
    elif name == 'C-M11':
        s = replace(s, '            atomicStore!(MemoryOrder.raw)(entry.childPending, 1u);', '            // missing activation custody reset')
    elif name == 'C-M12':
        a = s.index('private void pushChildReturn'); b = s.index('private void releaseMember', a)
        v = replace(s[a:b], '        head = atomicLoad!(MemoryOrder.raw)(*returns);', '        // failed CAS keeps stale expected head')
        s = s[:a] + v + s[b:]
    elif name == 'C-M13':
        s = replace(s, '        if (park.resumeCount == 0) park.resumeTail = null;', '        // stale tail after a full drain')
    elif name == 'C-M14':
        a = s.index('private struct ResumeBodies');b = s.index('/// Opaque',a)
        v = replace(s[a:b], '    @property ResumeBodies save() nothrow @nogc { return this; }', '    @property ResumeBodies save() nothrow @nogc\n    { auto copy = this; copy.cursor = park.resumeHead; return copy; } // aliases detached FIFO cursor')
        s = s[:a] + v + s[b:]
    else: raise ValueError(name)
    return s,t

cases = [
 ('C-M1','cont-basic',False), ('C-M2','cont-yield',False),
 ('C-M3','cont-descendants-first',False), ('C-M4','cont-activation-first',False),
 ('C-M5a','cont-hold-before-table',False), ('C-M5b','cont-hold-after-custody',True),
 ('C-M6','cont-done-tails',False), ('C-M7','cont-final-retire',False), ('C-M7b','cont-final-retire',False),
 ('C-M8','cont-fifo-drain-reuse',True), ('C-M8b','cont-resume-admission',False),
 ('C-M9a','cont-unschedule-before',True), ('C-M9b','cont-unschedule-after',True),
 ('C-M10','cont-results',False), ('C-M11','cont-multi-gen',False),
 ('C-M12','cont-return-cas-retry',False), ('C-M13','cont-fifo-return-overlap',False), ('C-M14','cont-fifo-partial',False)]
selected = set(sys.argv[1:]); outcomes = []
try:
 for name, selector, tsan in cases:
    if selected and name not in selected: continue
    assert runtime.read_text() == canonical and tests.read_text() == canonical_tests
    mutant, mutant_tests = mutate(name)
    patch = ''.join(difflib.unified_diff(canonical.splitlines(True),mutant.splitlines(True),fromfile='a/actors/private_actor.d',tofile='b/actors/private_actor.d'))
    if mutant_tests != canonical_tests:
        patch += ''.join(difflib.unified_diff(canonical_tests.splitlines(True),mutant_tests.splitlines(True),fromfile='a/private_actor_tests/main.d',tofile='b/private_actor_tests/main.d'))
    (evidence/(name+'.patch')).write_text(patch)
    runtime.write_text(mutant);tests.write_text(mutant_tests)
    binary='/tmp/private-continuation-mutant-'+name
    command=['ldc2','-g','-O1','-d-version=AntfarmWriteAuditHooks','-d-version=AntfarmActorTestHooks','-I.']
    if tsan: command+=['-fsanitize=thread','-d-version=TSan']
    command+=['private_actor_tests/main.d','antfarm.d','antfarm_allocation.d','actors/actor.d','actors/wave.d','actors/private_actor.d','-of='+binary]
    start=time.monotonic(); build=subprocess.run(command,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
    log='Canonical runtime SHA256: '+hashlib.sha256(canonical.encode()).hexdigest()+'\nBuild: '+' '.join(command)+f'\nBuild exit: {build.returncode}\n'+build.stdout
    if build.returncode:
        (evidence/(name+'.log')).write_text(log);print(name,'BUILD FAILED',build.stdout,flush=True);break
    run=['timeout','45',binary,selector];p=subprocess.run(run,env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True)
    log+='Run: ANTFARM_HUGE_PAGES=0 TSAN_OPTIONS=history_size=7:halt_on_error=1 '+' '.join(run)+f'\nExit: {p.returncode}\nBuild+run elapsed: {time.monotonic()-start:.3f}s\n'+p.stdout
    (evidence/(name+'.log')).write_text(log)
    print(name,'CAUGHT' if p.returncode else 'SURVIVED',p.returncode,p.stdout[:700],flush=True)
    outcomes.append(dict(name=name,selector=selector,tsan=tsan,exit=p.returncode,elapsed=round(time.monotonic()-start,3)))
    runtime.write_text(canonical); tests.write_text(canonical_tests)
    if p.returncode == 0: break
finally:
 runtime.write_text(canonical);tests.write_text(canonical_tests)
 (evidence/'outcomes-latest.json').write_text(json.dumps(outcomes,indent=2)+'\n')
 print('Canonical runtime/tests restored; SHA256',hashlib.sha256(runtime.read_bytes()).hexdigest(),flush=True)
