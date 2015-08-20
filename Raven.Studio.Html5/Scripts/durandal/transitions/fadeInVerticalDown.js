define(['durandal/system', 'transitions/transitionHelper'], function (system, helper) {
	var settings = {
		inAnimation: 'fadeInDownBig',
		outAnimation: 'fadeOutUpBig'
	},
    fadeInVerticalDown = function (context) {
        system.extend(context, settings);
        return helper.create(context);
    };

	return fadeInVerticalDown;
});