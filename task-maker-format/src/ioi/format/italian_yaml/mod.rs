//! The `italian_yaml` format is defined by [`cms`](https://cms.readthedocs.io/en/v1.4/External%20contest%20formats.html#italian-import-format)
//! and it's the most used format in the Italian Olympiads.
//!
//! # `gen/GEN` format
//! Here it's provided the definition of the format of the `gen/GEN` file, as interpreted by
//! task-maker. The aim is to be as compatible as possible to the format accepted by `cmsMake` and
//! `cmsImportTask`.
//!
//! The `gen/GEN` file describes how the input files of the testcases should be generated and how
//! the subtasks are composed. The formal definition of the format can be found looking at the
//! [parsing grammar](https://github.com/edomora97/task-maker-rust/blob/master/task-maker-format/src/ioi/format/italian_yaml/GEN.pest).
//! The format is described here informally.
//!
//! Each line of that file can be of one of the following types:
//!
//! - if the line starts with `# ` (number sign followed by a space) it is a comment and will be
//!   ignored. Example: `# this is a comment`
//! - if the line starts with `#ST:` it is the marker of the start of a new subtask. Following the
//!   column there is the score of the subtask, an integer number. Example: `#ST: 20`, meaning that
//!   all the following testcases, before the next `#ST:` are grouped in a single subtask worth 20
//!   points.
//! - if the line starts with `#COPY:` it is a testcase with a static input file, meaning that the
//!   input file will be simply copied from the path specified after the column. Example:
//!   `#COPY: gen/hardcoded.in`. The path is relative to the task root directory and the file must
//!   exists and be readable from task-maker. The path should not contain spaces.
//! - non-empty lines not starting with `#` defines a new testcases each. Each line contains
//!   command line arguments to pass to the generator executable. The generator should be named
//!   `gen/generator.*` or `gen/generatore.*`. Example: `1 2 3`, the generator will be invoked
//!   passing the three arguments.
//!
//! If a line contains a `#`, all the characters following it (`#` included) will be ignored as they
//! are considered comments. Example: `1 2 3 # inline comment`.
//!
//! If no `#ST` lines are present, a single subtask worth 100 points is automatically added.
//!
//! ## Full example of `gen/GEN`
//!
//! ```text
//! # N     M       seed
//! #ST: 0
//! #COPY: gen/example1.txt
//! #COPY: gen/example2.txt
//!
//! #ST: 30
//! 10      1000    101
//! 20      1000    102
//! 1       1       103 # corner case!
//!
//! #ST: 70
//! 1000    10000   201
//! 2000    10000   202
//! ```
//!
//! In this example the first line is ignored since it's a comment, then the definition of a subtask
//! of zero points starts. The content of that subtask are 2 hardcoded input files, for example the
//! sample cases in the text statement.
//!
//! Then a new subtask of 30 points starts, it's composed of 3 testcases. Note that the comment on
//! the third will be ignored and just the 3 numbers will be passed to the generator.
//!
//! Then there is a third subtask with 2 testcases.
//!
//! # `gen/cases.gen` format
//!
//! The `gen/GEN` format is pretty limited regarding some important aspects of task preparation. For
//! example it allows you to use just a single generator and a single validator. A new format, not
//! yet officially supported by `cms` (but workaround exists!), is here described.
//!
//! The formal definition of the format can be found in the
//! [parsing grammar](https://github.com/edomora97/task-maker-rust/blob/master/task-maker-format/src/ioi/format/italian_yaml/cases.gen.pest).
//! An informal explanation is here provided.
//!
//! Similarly to `gen/GEN`, each line is independent and can be one of the following:
//! - if it starts with `#` it is a comment and will be ignored.
//! - if it starts with `:` it is a command and what follows is described below.
//! - otherwise if it does not start with `#` nor `:`, it's a simple testcase definition. The
//!   testcases will be generated using the _current generator_, which is either the default one or
//!   the overridden one in the current subtask.
//!
//! If a line contains a `#`, all the characters following it (`#` included) will be ignored as they
//! are considered comments. Example: `1 2 3 # inline comment`.
//!
//! ## Backwards compatibility with cms
//! Since the importers of cms do not yet support the `cases.gen` format, there is a workaround that
//! works as follows: when the task is built task-maker will create a _fake_ `gen/GEN` with just the
//! metadata needed by `cmsImportTask` (like the number of testcases and subtasks).
//!
//! This file also contains some comments that may be found useful for debugging the generations.
//!
//! That file will be ignore by task-maker if the `cases.gen` file is present and will be removed
//! with `--clean`. Note that you should not edit that file because it will be overwritten the next
//! time task-maker will be launched. To keep that file you should remove the comment containing
//! `tm-allow-delete`.
//!
//! ## `cases.gen` commands
//! The lines starting with a column `:` are commands. What follows the column is the actual command
//! which can be of many types.
//!
//! ### `: GEN name path [args...]`
//! This command registers a new generator for the task. The generator is referenced in the file
//! with the specified `name`, a unique string without spaces. That generator's source file will be
//! found at the specified `path`. The optional following `args...` define the command line
//! arguments of this generator, they will be used for validating the constraints (see below).
//!
//! If the name is `default` the generator will be considered the default one and will be used when
//! no specific generator is selected.
//!
//! Example: `: GEN default gen/generator.py N M seed` defines the _default_ generator for the task,
//! whose source file is at `gen/generator.py` (relative to the task's root directory) and will
//! accept 3 arguments named `N`, `M` and `seed`. When this generator will be used the variables
//! `$N`, `$M` and `$seed` will be set and available for the constraint evaluation.
//!
//! Example: `: GEN line gen/line.py` defines a new generator named `line` whose parameters are not
//! known and won't be validated.
//!
//! ### `: VAL name path [args...]`
//! This command registers a new validator for the task. The semantics of the command are the same
//! of `: GEN name path [args...]`, including the behaviour of the `default` name.
//!
//! If no arguments are specified for the validator the default behaviour is to pass the variables
//! `$INPUT` and `$ST_NUM` (similar to `gen/GEN`, but the subtask is 0-based).
//!
//! Note that, differently than `: GEN` the arguments of the validator do not define new variables,
//! instead defines which parameters task-maker will pass to the validator. Because of that the
//! variables should be prefixed with `$` (the variables are used, not declared).
//!
//! Note that the `name` must be unique among the validators, but it can be the same of the one of
//! a generator.
//!
//! Example: `: VAL default gen/validator.py` defines the _default_ validator for the task.
//!
//! Example: `: VAL line gen/val_line.py $INPUT $ST_NUM` defines the `line` validator which takes 2
//! arguments, the same as the default behaviour, but using the variables to specify them.
//!
//! ### `: GEN name`
//! This command overrides the default generator for the current subtask, meaning that all the
//! testcases in the current subtask, following this command, will use the generator named `name` by
//! default. A generator named `name` must have been previously defined.
//!
//! Example: `: GEN line` sets the current generator to `line`.
//!
//! ### `: VAL name`
//! This command overrides the default validator for the current subtask, meaning that all the
//! testcases in the current subtask, following this command, will use the validator named `name`.
//! A validator named `name` must have been previously defined.
//!
//! Example: `: VAL line` sets the current validator to `line`.
//!
//! ### `: CONSTRAINT operand (operator operand)+`
//! This command adds a constraint that validates the parameters of the testcases. The arguments of
//! `: CONSTRAINT` form an expression that is an inequality (with equalities allowed) between
//! constants and variables. When a testcase is defined using a generator with the arguments known,
//! all those variables become defined and will be checked with all the constraints.
//!
//! The operators available are: < <= > >= =, but note that the inequalities must have the same
//! direction (cannot mix < and >).
//!
//! Constraints defined before the first subtask will be used for all the subtasks. Constraints
//! defined inside a subtask will be used only for that subtask.
//!
//! Example: `:CONSTRAINT 0 <= $N < $M <= 1000000` will check that the variables `$N` and `$M` are
//! between 0 and 1000000 and `$N` is smaller than `$M`.
//!
//! ### `: SUBTASK score [description]`
//! This command marks the start of a new subtask, just like how `#ST` in `gen/GEN` did. The score
//! can be a simple floating point number (either an integer or an integer.integer). The description
//! that follows is optional and will be included in the subtask metadata.
//!
//! When a new subtask is started the generator and validator will be reset to the default ones.
//!
//! Example: `: SUBTASK 40 All the nodes are in a line` defines a new subtask worth 40 points, with
//! the provided description.
//!
//! ### `: COPY path`
//! This command creates a new testcase coping the input file from the specified path, relative to
//! the task root directory. The file will be validated using the current validator of the subtask.
//!
//! Example: `: COPY gen/hardcoded.in`
//!
//! ### `: RUN name args...`
//! This command creates a new testcase using the generator named `name`, passing to it the
//! following arguments. The generator must have been previously defined.
//!
//! If the generator has the definition of its parameters, they will be used for assigning the
//! variables for checking the constraints. All the constraints must be satisfied for each testcase.
//!
//! The arguments provided are parsed with a shell lexer, meaning that `"` and `'` have a semantic
//! value (the same as a shell). Unlike `gen/GEN` you can pass arguments with a space in it using
//! the quotes.
//!
//! Example: `: RUN line 1 2 3` will run the `line` generator passing the three integers as
//! arguments.
//!
//! ## Testcase definition
//! Similarly to `gen/GEN` lines that are not commands nor comments are simple testcase definition.
//! Their semantics is the same of `: RUN default args...`.
//!
//! ## Automatic variables
//! In the constraint evaluation and in the validator argument specification all the variables
//! obtained from the parsing of the generator's arguments will be available. Also some automatic
//! variables will be available:
//! - `$ST_NUM`: the 0-based index of the subtask
//! - `$ST_DESCRIPTION`: the description of the subtask
//! - `$TC_NUM`: the 0-based index of the testcase
//! - `$INPUT` _(only for validators)_: the name of the file to validate
//!
//! ## Full example of `cases.gen`
//! ```text
//! : GEN default gen/generator.py N M seed
//! : GEN line gen/line.py N seed
//! : GEN hard gen/hard.py
//!
//! : VAL default gen/validator.py
//! : VAL line gen/val_line.py $INPUT $ST_NUM # same as default
//!
//! : CONSTRAINT 1 <= $N <= 1000
//! : CONSTRAINT 1 <= $M <= 1000000
//!
//! : SUBTASK 0 Examples
//! : COPY gen/example1.in
//! : COPY gen/example2.in
//!
//! : SUBTASK 30 Nodes are in a line
//! : GEN line
//! : VAL line
//! : CONSTRAINT $N <= 500
//! 500   101
//! 500   102
//!
//! : SUBTASK 70
//! 1000   1000      201
//! 1000   1000000   202
//! : RUN hard 1000 1000 95% 12.3 203
//! ```
//!
//! In this example 3 generators and 2 validators are defined, named `default`, `line`, `hard` and
//! `default`, `line` respectively. The `default` and `line` generators have their parameters
//! specified, they will be used for the constraints check; the `hard` generator arguments won't be
//! validated.
//!
//! Note that the second validator has the arguments specified, and they are the same as the default
//! ones. Also note that the inline comment will be ignored.
//!
//! This file defines 3 subtasks, worth 0, 30 and 70 points each.
//!
//! The first subtask contains 2 testcases whose file will be simply copied from the specified
//! paths.
//!
//! The second subtask has 2 testcases each generated with the `line` generator and validated with
//! the `line` validator. Note that there is an additional constraint for the subtask, it will be
//! checked only in this subtask.
//!
//! The third subtask will use the `default` generator and validator, except for the last testcase
//! which will use the `hard` one. Note that since the `hard` generator does not have the argument
//! specification, its parameters won't be checked. Also note that the constraint `$N <= 500` won't
//! be checked because it was scoped only to the second subtask.
//! The subtask also does not have a description, the default one (`Subtask 2`) will be used.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use anyhow::{bail, Error};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub(crate) use cases_gen::{is_gen_gen_deletable, TM_ALLOW_DELETE_COOKIE};
use task_maker_lang::GraderMap;

use crate::ioi::sanity_checks::get_sanity_checks;
use crate::ioi::TM_VALIDATION_FILE_NAME;
use crate::ioi::{
    make_booklets, Checker, IOITask, InputValidator, OutputGenerator, SubtaskId, SubtaskInfo,
    TaskType, TestcaseId, TestcaseInfo, TestcaseScoreAggregator,
};
use crate::ioi::{BatchTypeData, CommunicationTypeData};
use crate::{find_source_file, list_files, EvaluationConfig};

mod cases_gen;
mod gen_gen;
mod static_inputs;

/// Deserialized data from the task.yaml of a IOI format task.
#[derive(Debug, Serialize, Deserialize)]
struct TaskYAML {
    /// The name of the task (the short one).
    #[serde(alias = "nome_breve")]
    pub name: String,
    /// The title of the task (the long one).
    #[serde(alias = "nome")]
    pub title: String,
    /// The score type to use for this task.
    pub score_type: Option<String>,

    /// The time limit for the execution of the solutions, if not set it's unlimited.
    #[serde(alias = "timeout")]
    pub time_limit: Option<f64>,
    /// The memory limit in MiB of the execution of the solution, if not set it's unlimited.
    #[serde(alias = "memlimit")]
    pub memory_limit: Option<u64>,

    /// Whether this is an output only task. Defaults to false.
    #[serde(default = "bool::default")]
    #[serde(serialize_with = "python_bool_serializer")]
    #[serde(deserialize_with = "python_bool_deserializer")]
    pub output_only: bool,
    /// The input file for the solutions, usually 'input.txt' or '' (stdin). Defaults to `''`.
    #[serde(default = "default_infile")]
    pub infile: String,
    /// The output file for the solutions, usually 'output.txt' or '' (stdout). Defaults to `''`.
    #[serde(default = "default_outfile")]
    pub outfile: String,

    /// An integer that defines the difficulty of the task. Used only in booklet compilations.
    pub difficulty: Option<u8>,
    /// An integer that defines the level inside a _syllabus_ (for example for the Olympiads in
    /// Teams). Used only in booklet compilations.
    pub syllabuslevel: Option<u8>,

    /// Number of solution processes to spawn in parallel in a communication task.
    pub num_processes: Option<u8>,
}

/// The iterator item type when following the task input testcases.
#[derive(Debug, Clone)]
pub(crate) enum TaskInputEntry {
    /// Create a new subtask given its information.
    Subtask(SubtaskInfo),
    /// Create a new testcase inside the last subtask given its information. `Testcase` can be sent
    /// only after at least one `Subtask`.
    Testcase(TestcaseInfo),
}

/// Given the path to the task directory, try to parse the task inside of it assuming the format is
/// `italian_yaml`.
///
/// `italian_yaml` format is structured as follow:
/// * `task.yaml` - file with the task information
/// * `gen/` - folder with the generator and validator
///     * `generator.xxx` (also `generatore`)
///     * `validator.xxx` (also `valida`)
///     * `GEN` - subtask and testcase specifications
/// * `sol/` - folder with solutions, graders and stubs
///     * `solution.xxx` the official solution (also `soluzione`)
///     * other solutions with different names
/// * `check/` - folder with the checker (also `cor/`)
///     * `checker.xxx` (also `correttore`)
/// * `input/` - folder with the input files
/// * `output/` - folder with the output files
/// * `statement/` - folder with the statement (also `testo`)
///
/// A task must have a generator (and a GEN file) or the input files should be  put in `input/`.
/// The official solution must be present or the output files should be put in `output/`.
pub fn parse_task<P: AsRef<Path>>(
    task_dir: P,
    eval_config: &EvaluationConfig,
) -> Result<IOITask, Error> {
    let task_dir = task_dir.as_ref();
    let yaml: TaskYAML = serde_yaml::from_reader(fs::File::open(&task_dir.join("task.yaml"))?)?;
    debug!("The yaml is {:#?}", yaml);

    let map_file = |file: String| -> Option<PathBuf> {
        match file.as_ref() {
            "" => None,
            _ => Some(file.into()),
        }
    };
    let infile = map_file(yaml.infile.clone());
    let outfile = map_file(yaml.outfile.clone());

    let graders = list_files(task_dir, vec!["sol/grader.*", "sol/stub.*"]);
    let grader_map = Arc::new(GraderMap::new(graders));
    debug!("The graders are: {:#?}", grader_map);

    let task_type = if let Some(comm) = parse_communication_task_data(task_dir, &yaml)? {
        comm
    } else {
        parse_batch_task_data(task_dir, grader_map.clone())?
    };

    let gen_gen = task_dir.join("gen").join("GEN");
    let cases_gen = task_dir.join("gen").join("cases.gen");
    let output_generator: Box<dyn Fn(TestcaseId) -> OutputGenerator> =
        if let TaskType::Batch(_) = &task_type {
            Box::new(detect_output_generator(
                task_dir.to_path_buf(),
                grader_map.clone(),
            )?)
        } else {
            Box::new(|_| OutputGenerator::NotAvailable)
        };

    let inputs = if cases_gen.exists() {
        debug!("Parsing testcases from gen/cases.gen");
        let gen = cases_gen::CasesGen::new(&cases_gen, output_generator)?;
        if !eval_config.dry_run {
            gen.write_gen_gen()?;
        }
        gen.get_task_entries()
    } else if gen_gen.exists() {
        debug!("Parsing testcases from gen/GEN");
        gen_gen::parse_gen_gen(
            &gen_gen,
            detect_validator(task_dir.to_path_buf())?,
            output_generator,
        )?
    } else {
        debug!("Using testcases inside input/");
        static_inputs::static_inputs(
            task_dir,
            detect_validator(task_dir.to_path_buf())?,
            output_generator,
        )
        .collect()
    };

    let mut subtasks = HashMap::new();
    let mut last_subtask: Option<SubtaskInfo> = None;
    for input in inputs {
        match input {
            TaskInputEntry::Subtask(subtask) => {
                if let Some(last_subtask) = last_subtask.take() {
                    subtasks.insert(last_subtask.id, last_subtask);
                }
                last_subtask = Some(subtask);
            }
            TaskInputEntry::Testcase(testcase) => {
                last_subtask
                    .as_mut()
                    .expect("Testcase before Subtask")
                    .testcases
                    .insert(testcase.id, testcase);
            }
        }
    }
    // insert the last subtask to the map
    if let Some(subtask) = last_subtask.take() {
        subtasks.insert(subtask.id, subtask);
    }

    let mut task = IOITask {
        path: task_dir.into(),
        task_type,
        name: yaml.name,
        title: yaml.title,
        time_limit: yaml.time_limit,
        memory_limit: yaml.memory_limit,
        infile,
        outfile,
        testcase_score_aggregator: yaml
            .score_type
            .as_ref()
            .map(|s| TestcaseScoreAggregator::from_str(s))
            .unwrap_or_else(|| {
                if subtasks.len() == 1 {
                    Ok(TestcaseScoreAggregator::Sum)
                } else {
                    Ok(TestcaseScoreAggregator::Min)
                }
            })?,
        subtasks,
        grader_map,
        booklets: Vec::new(),
        difficulty: yaml.difficulty,
        syllabus_level: yaml.syllabuslevel,
        sanity_checks: Arc::new(get_sanity_checks(&eval_config.disabled_sanity_checks)),
        input_validator: detect_validator(task_dir.to_path_buf())?(0),
    };
    // split the creation of the task because make_booklets need an instance of Task
    if !eval_config.no_statement {
        task.booklets = make_booklets(&task, eval_config)?;
    }
    Ok(task)
}

/// Search for a valid input validator inside the task directory. Will return a function that, given
/// a subtask id, returns an `InputValidator` using that validator. If no validator is found,
/// `InputValidator::AssumeValid` is used.
fn detect_validator(task_dir: PathBuf) -> Result<impl Fn(SubtaskId) -> InputValidator, Error> {
    let mut validators = find_source_file(
        &task_dir,
        vec![
            "gen/validator.*",
            "gen/valida.*",
            "gen/validator",
            "gen/valida",
        ],
        &task_dir,
        None,
        Some(task_dir.join("bin").join("validator")),
    );
    if validators.len() > 1 {
        let paths = validators.iter().map(|s| s.name()).collect::<Vec<_>>();
        bail!("Multiple validators found: {:?}", paths);
    }
    let validator = validators.pop().map(Arc::new);
    debug!("Detected input validator: {:?}", validator);
    Ok(move |st: SubtaskId| -> InputValidator {
        if let Some(validator) = validator.as_ref() {
            InputValidator::Custom(
                validator.clone(),
                // for legacy support reasons the subtask is passed 1-based
                vec![TM_VALIDATION_FILE_NAME.to_string(), (st + 1).to_string()],
            )
        } else {
            InputValidator::AssumeValid
        }
    })
}

/// Search for a valid output generator (aka official solution) inside the task directory. Will
/// return a function that, given a testcase id, returns an `OutputGenerator` using that generator.
/// If no generator is found, `OutputGenerator::StaticFile` is used instead.
fn detect_output_generator(
    task_dir: PathBuf,
    grader_map: Arc<GraderMap>,
) -> Result<impl Fn(TestcaseId) -> OutputGenerator, Error> {
    let mut official_solutions = find_source_file(
        &task_dir,
        vec![
            "sol/solution.*",
            "sol/soluzione.*",
            "sol/solution",
            "sol/soluzione",
        ],
        &task_dir,
        Some(grader_map),
        Some(task_dir.join("bin").join("official_solution")),
    );
    if official_solutions.len() > 1 {
        let paths = official_solutions
            .iter()
            .map(|s| s.name())
            .collect::<Vec<_>>();
        bail!("Multiple official solutions found: {:?}", paths);
    }
    let official_solution = official_solutions.pop().map(Arc::new);
    debug!("Detected output generator: {:?}", official_solution);
    let output_directory = task_dir.join("output");
    Ok(move |tc: TestcaseId| -> OutputGenerator {
        if let Some(solution) = official_solution.as_ref() {
            OutputGenerator::Custom(solution.clone(), vec![])
        } else {
            OutputGenerator::StaticFile(output_directory.join(format!("output{}.txt", tc)))
        }
    })
}

/// Parse the task components relative to the batch task type.
fn parse_batch_task_data(task_dir: &Path, grader_map: Arc<GraderMap>) -> Result<TaskType, Error> {
    let mut checkers = find_source_file(
        task_dir,
        vec!["check/checker.*", "cor/correttore.*"],
        task_dir,
        None,
        Some(task_dir.join("check").join("checker")),
    );
    if checkers.len() > 1 {
        let paths = checkers.iter().map(|s| s.name()).collect::<Vec<_>>();
        bail!("Multiple checkers found: {:?}", paths)
    }
    let checker = checkers
        .pop()
        .map(|mut c| {
            // always copy the custom checker
            c.copy_exe();
            // Link the checker statically. This makes sure that it will work also outside this machine.
            c.link_static();
            Checker::Custom(Arc::new(c))
        })
        .unwrap_or(Checker::WhiteDiff);

    let official_solution = match detect_output_generator(task_dir.to_path_buf(), grader_map)?(0) {
        gen @ OutputGenerator::Custom(_, _) => Some(gen),
        _ => None,
    };
    Ok(TaskType::Batch(BatchTypeData {
        output_generator: official_solution,
        checker,
    }))
}

/// Parse the task components relative to the communication task type.
fn parse_communication_task_data(
    task_dir: &Path,
    yaml: &TaskYAML,
) -> Result<Option<TaskType>, Error> {
    let mut managers = find_source_file(
        task_dir,
        vec!["check/manager.*", "cor/manager.*"],
        task_dir,
        None,
        Some(task_dir.join("bin").join("manager")),
    );
    if managers.len() > 1 {
        let paths = managers.iter().map(|s| s.name()).collect::<Vec<_>>();
        bail!("Multiple managers found: {:?}", paths);
    }
    let mut manager = if let Some(manager) = managers.pop() {
        manager
    } else {
        return Ok(None);
    };
    // Link the manager statically. This makes sure that it will work also outside this machine.
    manager.link_static();

    Ok(Some(TaskType::Communication(CommunicationTypeData {
        manager: Arc::new(manager),
        num_processes: yaml.num_processes.unwrap_or(1),
    })))
}

/// Serializer of a boolean using the python syntax:
/// - `true` -> `True`
/// - `false` -> `False`
#[allow(clippy::trivially_copy_pass_by_ref)]
fn python_bool_serializer<S>(val: &bool, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    if *val {
        ser.serialize_str("True")
    } else {
        ser.serialize_str("False")
    }
}

/// Deserializer of a boolean using the python syntax:
/// - `True` -> `true`
/// - `False` -> `false`
/// - other -> error
fn python_bool_deserializer<'de, D>(deser: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    let val = String::deserialize(deser)?;
    if val == "True" {
        Ok(true)
    } else if val == "False" {
        Ok(false)
    } else {
        Err(Error::custom("invalid bool, either True or False"))
    }
}

/// The default value for the `infile` field of task.yaml.
fn default_infile() -> String {
    "input.txt".into()
}

/// The default value for the `outfile` field of task.yaml.
fn default_outfile() -> String {
    "output.txt".into()
}
