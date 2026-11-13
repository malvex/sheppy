# Changelog

## 0.0.8

* feat: worker crash recovery. PR [#58](https://github.com/malvex/sheppy/pull/58) by [@malvex](https://github.com/malvex)
* fix: xgroup_create race condition in redis backend. PR [#59](https://github.com/malvex/sheppy/pull/59) by [@malvex](https://github.com/malvex)
* feat: sync queue. PR [#60](https://github.com/malvex/sheppy/pull/60) by [@malvex](https://github.com/malvex)
* ci: add more test variants. PR [#61](https://github.com/malvex/sheppy/pull/61) by [@malvex](https://github.com/malvex)
* refactor: remove anyio dependency. PR [#62](https://github.com/malvex/sheppy/pull/62) by [@malvex](https://github.com/malvex)
* chore: fix new mypy errors. PR [#69](https://github.com/malvex/sheppy/pull/69) by [@malvex](https://github.com/malvex)
* feat: add support for error_ttl and per-task ttl. PR [#66](https://github.com/malvex/sheppy/pull/66) by [@malvex](https://github.com/malvex)
* fix: workflows. PR [#70](https://github.com/malvex/sheppy/pull/70) by [@malvex](https://github.com/malvex)
* feat: task exceptions. PR [#71](https://github.com/malvex/sheppy/pull/71) by [@malvex](https://github.com/malvex)
* feat: add prestart hook option. PR [#76](https://github.com/malvex/sheppy/pull/76) by [@malvex](https://github.com/malvex)
* refactor: add support for redis-py >=8.0. PR [#77](https://github.com/malvex/sheppy/pull/77) by [@malvex](https://github.com/malvex)
* feat: declarative cron jobs via pyproject.toml. PR [#78](https://github.com/malvex/sheppy/pull/78) by [@malvex](https://github.com/malvex)
* refactor: add check to resolve_function. PR [#79](https://github.com/malvex/sheppy/pull/79) by [@malvex](https://github.com/malvex)
* chore: remove pytest.xfails. PR [#81](https://github.com/malvex/sheppy/pull/81) by [@malvex](https://github.com/malvex)
* refactor: make internal code private. PR [#82](https://github.com/malvex/sheppy/pull/82) by [@malvex](https://github.com/malvex)
* refactor: move workflow and SyncQueue into experimental module. PR [#83](https://github.com/malvex/sheppy/pull/83) by [@malvex](https://github.com/malvex)
* docs: add documentation for SyncQueue and workflows. PR [#84](https://github.com/malvex/sheppy/pull/84) by [@malvex](https://github.com/malvex)
* refactor: move experimental queue methods into experimental attribute. PR [#86](https://github.com/malvex/sheppy/pull/86) by [@malvex](https://github.com/malvex)
* feat: make Task model generic over its result type. PR [#87](https://github.com/malvex/sheppy/pull/87) by [@malvex](https://github.com/malvex)
* fix: missing logger handler. PR [#88](https://github.com/malvex/sheppy/pull/88) by [@malvex](https://github.com/malvex)
* fix: handle middleware errors correctly. PR [#89](https://github.com/malvex/sheppy/pull/89) by [@malvex](https://github.com/malvex)
* fix: retry off-by-one. PR [#90](https://github.com/malvex/sheppy/pull/90) by [@malvex](https://github.com/malvex)
* fix: preserve variadic task arguments. PR [#92](https://github.com/malvex/sheppy/pull/92) by [@malvex](https://github.com/malvex)
* fix: preserve user timeout errors. PR [#93](https://github.com/malvex/sheppy/pull/93) by [@malvex](https://github.com/malvex)
* fix: keep scheduled retries non-terminal. PR [#95](https://github.com/malvex/sheppy/pull/95) by [@malvex](https://github.com/malvex)
* fix: preserve cron task config. PR [#96](https://github.com/malvex/sheppy/pull/96) by [@malvex](https://github.com/malvex)
* docs: cleanup. PR [#97](https://github.com/malvex/sheppy/pull/97) by [@malvex](https://github.com/malvex)
* fix: restore asyncer compatibility. PR [#98](https://github.com/malvex/sheppy/pull/98) by [@malvex](https://github.com/malvex)
* add tests for python 3.15. PR [#99](https://github.com/malvex/sheppy/pull/99) by [@malvex](https://github.com/malvex)
* feat: add json worker logs. PR [#100](https://github.com/malvex/sheppy/pull/100) by [@malvex](https://github.com/malvex)
* fix: timeouting tests. PR [#101](https://github.com/malvex/sheppy/pull/101) by [@malvex](https://github.com/malvex)
* feat: resolve backend TTL defaults at enqueue. PR [#102](https://github.com/malvex/sheppy/pull/102) by [@malvex](https://github.com/malvex)
* feat: improve fastapi.Depends feature parity. PR [#103](https://github.com/malvex/sheppy/pull/103) by [@malvex](https://github.com/malvex)
* feat: improve task list filtering and sorting. PR [#104](https://github.com/malvex/sheppy/pull/104) by [@malvex](https://github.com/malvex)
* refactor: rename MemoryBackend to InMemoryBackend. PR [#105](https://github.com/malvex/sheppy/pull/105) by [@malvex](https://github.com/malvex)
* feat: return tasks from queue methods. PR [#106](https://github.com/malvex/sheppy/pull/106) by [@malvex](https://github.com/malvex)
* refactor: move fastapi router to experimental. PR [#107](https://github.com/malvex/sheppy/pull/107) by [@malvex](https://github.com/malvex)
* chore: add uv.lock. PR [#108](https://github.com/malvex/sheppy/pull/108) by [@malvex](https://github.com/malvex)
* fix: task retry. PR [#110](https://github.com/malvex/sheppy/pull/110) by [@malvex](https://github.com/malvex)
* fix: discard undecodable stream entries instead of failing the batch. PR [#111](https://github.com/malvex/sheppy/pull/111) by [@tishun](https://github.com/tishun)

## 0.0.7

* fix: acknowledge rate limited tasks. PR [#53](https://github.com/malvex/sheppy/pull/53) by [@malvex](https://github.com/malvex)
* feat: add slot-based staggering for rate-limited task scheduling. PR [#53](https://github.com/malvex/sheppy/pull/53) by [@malvex](https://github.com/malvex)

## 0.0.6

* chore: bump minimal typer version to 0.19.0. PR [#44](https://github.com/malvex/sheppy/pull/44) by [@malvex](https://github.com/malvex)
* feat: implement workflow support. PR [#45](https://github.com/malvex/sheppy/pull/45) by [@malvex](https://github.com/malvex)
* feat: rate limits. PR [#47](https://github.com/malvex/sheppy/pull/47) by [@malvex](https://github.com/malvex)

## 0.0.5

* chore: speed up tests. PR [#26](https://github.com/malvex/sheppy/pull/26) by [@malvex](https://github.com/malvex)
* refactor: replace task.completed with task.status. PR [#27](https://github.com/malvex/sheppy/pull/27) by [@malvex](https://github.com/malvex)
* refactor(TaskProcessor): use protocols and improve middleware. PR [#28](https://github.com/malvex/sheppy/pull/28) by [@malvex](https://github.com/malvex)
* docs: rename example function to prevent confusion. PR [#29](https://github.com/malvex/sheppy/pull/29) by [@malvex](https://github.com/malvex)
* refactor(TestQueue): remove assert functions. PR [#30](https://github.com/malvex/sheppy/pull/30) by [@malvex](https://github.com/malvex)
* refactor: move mkdocs install from Taskfile.yml to pyproject.yml. PR [#31](https://github.com/malvex/sheppy/pull/31) by [@malvex](https://github.com/malvex)
* chore: `__init__.py` files. PR [#32](https://github.com/malvex/sheppy/pull/32) by [@malvex](https://github.com/malvex)
* docs: remove types from docstrings. PR [#33](https://github.com/malvex/sheppy/pull/33) by [@malvex](https://github.com/malvex)
* feat(queue): automatically infer backend from URL string. PR [#34](https://github.com/malvex/sheppy/pull/34) by [@malvex](https://github.com/malvex)
* chore: rename `utils/` to `_utils/`. PR [#35](https://github.com/malvex/sheppy/pull/35) by [@malvex](https://github.com/malvex)
* chore(cli): use Annotated for command args. PR [#37](https://github.com/malvex/sheppy/pull/37) by [@malvex](https://github.com/malvex)
* docs: regenerate CLI docs. PR [#38](https://github.com/malvex/sheppy/pull/38) by [@malvex](https://github.com/malvex)
* feat: add support for env variables. PR [#41](https://github.com/malvex/sheppy/pull/41) by [@malvex](https://github.com/malvex)
* feat: allow dynamically created tasks. PR [#42](https://github.com/malvex/sheppy/pull/42) by [@malvex](https://github.com/malvex)

## 0.0.4

* refactor(backend): use xack+xdel instead of xackdel. PR [#20](https://github.com/malvex/sheppy/pull/20) by [@malvex](https://github.com/malvex)
* feat: add dependency override to TestQueue. PR [#21](https://github.com/malvex/sheppy/pull/21) by [@malvex](https://github.com/malvex)
* feat: add task timeout. PR [#22](https://github.com/malvex/sheppy/pull/22) by [@malvex](https://github.com/malvex)
* fix(worker): improve logic to fix slow shutdown. PR [#23](https://github.com/malvex/sheppy/pull/23) by [@malvex](https://github.com/malvex)
* feat: add FastAPI APIRouter. PR [#24](https://github.com/malvex/sheppy/pull/24) by [@malvex](https://github.com/malvex)
* feat: add local backend and improve memory backend. PR [#25](https://github.com/malvex/sheppy/pull/25) by [@malvex](https://github.com/malvex)

## 0.0.3

* Multiple bugfixes and small refactor. PR [#18](https://github.com/malvex/sheppy/pull/18) by [@malvex](https://github.com/malvex)
* Replace 'self: Task' magic with 'CURRENT_TASK' to fix issues with IDE typing. PR [#13](https://github.com/malvex/sheppy/pull/13) by [@malvex](https://github.com/malvex)
* feat: add basic task chaining. PR [#15](https://github.com/malvex/sheppy/pull/15) by [@malvex](https://github.com/malvex)
* refactor: remove intermediary TaskInternal model. PR [#19](https://github.com/malvex/sheppy/pull/19) by [@malvex](https://github.com/malvex)

## 0.0.2

* Feat: Add documentation. PR [#4](https://github.com/malvex/sheppy/pull/4) by [@malvex](https://github.com/malvex)
* fix: mypy errors. PR [#16](https://github.com/malvex/sheppy/pull/16) by [@malvex](https://github.com/malvex)
* chore(ci): add support for python 3.14. PR [#17](https://github.com/malvex/sheppy/pull/17) by [@malvex](https://github.com/malvex)
* feat(worker): add support for --max-prefetch. PR [#12](https://github.com/malvex/sheppy/pull/12) by [@malvex](https://github.com/malvex)
* feat(models): Add exception name into task.error attribute. PR [#11](https://github.com/malvex/sheppy/pull/11) by [@malvex](https://github.com/malvex)
* fix: Task exception was never retrieved if return annotation validation fails. PR [#14](https://github.com/malvex/sheppy/pull/14) by [@malvex](https://github.com/malvex)

## 0.0.1

Initial release.
