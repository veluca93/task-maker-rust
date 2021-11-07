//! The Terry task format.
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Error;
use rand::Rng;
use serde::{Deserialize, Serialize};
use typescript_definitions::TypeScriptify;

pub use task_info::*;

use crate::sanity_checks::SanityChecks;
use crate::terry::curses_ui::CursesUI;
use crate::terry::dag::{Checker, InputGenerator, InputValidator, Solution};
use crate::terry::format::parse_task;
use crate::terry::ui_state::UIState;
use crate::ui::{JsonUI, PrintUI, RawUI, SilentUI, UIMessage, UIMessageSender, UIType, UI};
use crate::{
    list_files, EvaluationConfig, EvaluationData, SourceFile, TaskFormat, TaskInfo, UISender,
};

mod curses_ui;
mod dag;
pub(crate) mod finish_ui;
mod format;
pub(crate) mod sanity_checks;
pub(crate) mod task_info;
pub(crate) mod ui_state;

/// The type of the seed of a generator for an input file.
pub type Seed = u64;

/// Information about a generic Terry task.
#[derive(Debug, Clone, Serialize, Deserialize, TypeScriptify)]
pub struct TerryTask {
    /// Path of the directory of the task.
    pub path: PathBuf,
    /// The name of the task (the short one).
    pub name: String,
    /// The title of the task (the long one).
    pub description: String,
    /// The maximum score for this task.
    pub max_score: f64,

    /// The generator of input files of this task.
    #[serde(skip_serializing)]
    pub generator: InputGenerator,
    /// The validator of input files of this task.
    #[serde(skip_serializing)]
    pub validator: Option<InputValidator>,
    /// The checker of input/output files of this task.
    #[serde(skip_serializing)]
    pub checker: Checker,
    /// The official solution of this task, if any. Will be compiled and placed in the sandbox of
    /// the generation/validation/checking.
    #[serde(skip_serializing)]
    pub official_solution: Option<Arc<SourceFile>>,
    /// The sanity checks attached to this task. Wrapped in Arc since `SanityChecks` is not Clone.
    /// It's also not `Serialize` nor `Deserialize`, all the sanity checks will be lost on
    /// serialization.
    #[serde(skip_serializing, skip_deserializing)]
    pub sanity_checks: Arc<SanityChecks<TerryTask>>,
}

/// The output of the checker for a solution.
#[derive(Debug, Clone, Serialize, Deserialize, TypeScriptify)]
pub struct SolutionOutcome {
    /// The score normalized from 0.0 to 1.0.
    pub score: f64,
    /// The validation outcome of the solution.
    pub validation: SolutionValidation,
    /// The feedback outcome of the solution.
    pub feedback: SolutionFeedback,
}

/// The validation part of the outcome of a solution.
#[derive(Debug, Clone, Serialize, Deserialize, TypeScriptify)]
pub struct SolutionValidation {
    /// The validation of the test cases, in the same order as the input.
    pub cases: Vec<SolutionValidationCase>,
    /// The alerts sent by the checker regarding the validation.
    pub alerts: Vec<SolutionAlert>,
}

/// The validation outcome of a test case.
#[derive(Debug, Clone, Serialize, Deserialize, TypeScriptify)]
pub struct SolutionValidationCase {
    /// The status of the testcase.
    pub status: CaseStatus,
    /// An optional message associated to the test case.
    pub message: Option<String>,
}

/// The possible statuses of the validation of a test case.
#[derive(Debug, Clone, Serialize, Deserialize, TypeScriptify)]
#[serde(rename_all = "lowercase")]
pub enum CaseStatus {
    /// The testcase is not present in the output file.
    Missing,
    /// The testcase is present and correctly parsed.
    Parsed,
    /// The testcase is present but its format is invalid.
    Invalid,
}

/// The feedback part of the outcome.
#[derive(Debug, Clone, Serialize, Deserialize, TypeScriptify)]
pub struct SolutionFeedback {
    /// The feedback of each testcase, in the same order as the input.
    pub cases: Vec<SolutionFeedbackCase>,
    /// The alerts sent by the checker regarding the feedback.
    pub alerts: Vec<SolutionAlert>,
}

/// The feedback of a test case.
#[derive(Debug, Clone, Serialize, Deserialize, TypeScriptify)]
pub struct SolutionFeedbackCase {
    /// Whether this testcase is correct.
    pub correct: bool,
    /// An optional message associated to the test case.
    pub message: Option<String>,
}

/// A message with an associated severity.
#[derive(Debug, Clone, Serialize, Deserialize, TypeScriptify)]
pub struct SolutionAlert {
    /// The severity of the alert message.
    pub severity: String,
    /// The content of the alert.
    pub message: String,
}

impl TerryTask {
    /// Try to make a `Task` from the specified path. Will return `Err` if the format of the task
    /// is not Terry or if the task is corrupted and cannot be parsed.
    pub fn new<P: AsRef<Path>>(
        path: P,
        eval_config: &EvaluationConfig,
    ) -> Result<TerryTask, Error> {
        parse_task(path.as_ref(), eval_config)
    }

    /// Check if in the provided path there could be a Terry-like task.
    pub fn is_valid<P: AsRef<Path>>(path: P) -> bool {
        path.as_ref().join("task.yaml").exists()
    }
}

impl TaskFormat for TerryTask {
    fn path(&self) -> &Path {
        &self.path
    }

    fn ui(&self, ui_type: &UIType) -> Result<Box<dyn UI>, Error> {
        match ui_type {
            UIType::Raw => Ok(Box::new(RawUI::new())),
            UIType::Json => Ok(Box::new(JsonUI::new())),
            UIType::Silent => Ok(Box::new(SilentUI::new())),
            UIType::Print => Ok(Box::new(PrintUI::<UIState>::new())),
            UIType::Curses => Ok(Box::new(CursesUI::new(UIState::new(self))?)),
        }
    }

    fn build_dag(&self, eval: &mut EvaluationData, config: &EvaluationConfig) -> Result<(), Error> {
        eval.sender.send(UIMessage::TerryTask {
            task: Box::new(self.clone()),
        })?;
        self.sanity_checks.pre_hook(self, eval)?;
        let solutions = config.filter_solutions(&self.path, vec!["solutions/*"], None);
        let mut rng = rand::thread_rng();
        for solution in solutions {
            let seed = if let Some(seed) = config.seed {
                seed
            } else {
                rng.gen_range(0, 1 << 31)
            };
            let input_file = self.generator.generate_and_bind(
                eval,
                &solution,
                seed,
                self.official_solution.clone(),
            )?;
            let validation_file = if let Some(validator) = self.validator.as_ref() {
                Some(validator.validate_and_bind(
                    eval,
                    &solution,
                    input_file,
                    self.official_solution.clone(),
                )?)
            } else {
                None
            };
            let output_file =
                Solution::solve_and_bind(eval, &solution, input_file, validation_file)?;
            let sender = eval.sender.clone();
            let solution_path = solution.path.clone();
            self.checker.check_and_bind(
                eval,
                &solution,
                input_file,
                output_file,
                self.official_solution.clone(),
                move |outcome| {
                    sender.send(UIMessage::TerrySolutionOutcome {
                        solution: solution_path,
                        outcome: outcome.map_err(|e| format!("Invalid checker outcome: {}", e)),
                    })
                },
            )?;
        }
        Ok(())
    }

    fn sanity_check_post_hook(&self, ui: &mut UIMessageSender) -> Result<(), Error> {
        self.sanity_checks.post_hook(self, ui)
    }

    fn clean(&self) -> Result<(), Error> {
        let all_managers: HashSet<PathBuf> = list_files(&self.path, vec!["managers/*.*"])
            .iter()
            .map(|f| f.file_stem().unwrap().into())
            .collect();
        for maybe_generated in list_files(&self.path, vec!["managers/*.*.*"]) {
            let name = Path::new(maybe_generated.file_stem().unwrap());
            let name = Path::new(name.file_stem().unwrap());
            // if there is a file.X associated to file.Y.Z, remove file.Y.Z, e.g.:
            // in managers/ there is validator.py => all_managers includes "validator"
            //   maybe_generated == "validator.linux.x86_64"
            //   name == "validator"
            if all_managers.contains(name) {
                info!("Removing {}", maybe_generated.display());
                std::fs::remove_file(maybe_generated)?;
            }
        }
        // remove the bin/ folder
        let bin_path = self.path.join("bin");
        if bin_path.exists() {
            info!("Removing {}", bin_path.display());
            std::fs::remove_dir_all(bin_path)?;
        }
        Ok(())
    }

    fn task_info(&self) -> Result<TaskInfo, Error> {
        Ok(TaskInfo::Terry(task_info::TerryTaskInfo::new(self)?))
    }
}
