# restage
Semiautomatic two-stage McCode simulations for parameter scans.

## Motivation
Some instrument simulations are very costly. 
This is especially true for instruments with a large number of components. 
In order to reduce the runtime of a simulated parameter scan, it is possible to run the simulations in two stages.
In the first stage, the instrument is simulated up to a predefined point and the particle states are saved in an MCPL file. 
The output particle states are then used as input for the second stage, 
where the instrument is simulated from the predefined point to the end of the instrument.
On its own, such a split simulation *is detrimental* to the simulation runtime, 
since writing and reading the MCPL file takes time.

Where such a technique is useful, however, is when the first stage output can be reused 
for multiple second stage simulations.
This is the case, e.g., when a 'standard' sample-rotation scan is performed, 
where the sample is rotated around a single axis and the rest of the instrument is kept fixed.
In this case, the first stage simulation only needs to be performed once,
and the second stage simulations can be performed for each sample rotation angle.

## Installation
The `restage` module is installable via `pip`:
```bash
pip install restage
```

or directly from the source code:
```bash
pip install git+https://github.com/g5t/restage.git
```

## Usage
### `restage`
The `restage` module provides a function, `restage`, which takes a McStas instrument file as input.
The function will then parse the instrument file and generate new instrument files for the first and second stage simulations.
The new instrument files will be named `restaged_lower_<original instrument file name>.instr`,
and `restaged_upper_<original instrument file name>.instr`, respectively.

### `splitrun`
A more useful function, `splitrun`, is also provided by the `restage` module.
The `splitrun` function takes the same arguments as the `restage` function,
but will also run the simulations for the first and second stage.
The function will return the output of the second stage simulation.

The `splitrun` function can be used as a replacement for the `mcrun` function distributed as part of McStas.
One optional argument, `splitpoint`, is added to the `splitrun` function; it should be the name of
an `Arm` component inside the instrument file and defaults to `split_at`.
In contrast to `mcrun`, instrument parameters for `splitrun` are specified as 'MATLAB'-range style keyword arguments.
A valid range is inclusive of its end points and of the form `start:step:end` or `start:end` (with implicit `step=1`).

The `-N` argument of `mcrun` is removed since the number of scan steps is now determined by the range
of the scan parameter; this means that multiple scanned parameters must have ranges that agree on the number of steps.

The switch to MATLAB-style range specification is done to allow for flexible mesh scans, via the optional `-m` flag.
When the `-m` flag is specified, the scan parameters are interpreted as mesh scan parameters and the number of steps
along each mesh parameter is not constrained to be the same.

#### Example

As an example, for the instrument file `my_instrument.instr`
```
DEFINE INSTRUMENT my_instrument(sample_angle=0, sample_radius)
COMPONENT source = Source(...) AT (0, 0, 0) ABSOLUTE
COMPONENT guide = GuideGravity(l=10) AT (0, 0, 0.1) RELATIVE source
COMPONENT end_of_guide = Arm() AT (0, 0, 10) RELATIVE guide
COMPONENT slits = Slit(...) AT (0, 0, 0.2) RELATIVE end_of_guide
COMPONENT split_at = Arm() AT (0, 0, 0) RELATIVE slits
COMPONENT sample_pos = Arm() AT (0, 0, 0.2) RELATIVE split_at
COMPONENT sample = Sample(radius=sample_radius) AT (0, 0, 0) RELATIVE sample_pos 
                   ROTATED (0, sample_angle, 0) RELATIVE split_at
COMPONENT detector_arm = Arm() AT (0, 0, 0) RELATIVE sample_pos ROTATED (0, 45, 0) RELATIVE sample_pos
COMPONENT detector = Monitor(...) AT (0, 0, 2) RELATIVE detector_arm
END
```

the McStas `mcrun` command
```bash
mcrun my_instrument.instr -N 90 -n 1000000 -d /data/output sample_angle=1,90 sample_radius=10.0
```
can be replaced by the `splitrun` command
```bash
splitrun my_instrument.instr -n 1000000 -d /data/output sample_angle=1:90 sample_radius=10.0
```



